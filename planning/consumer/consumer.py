import pika
import mysql.connector
from mysql.connector import Error
import os
import logging
import xml.etree.ElementTree as ET
from datetime import datetime

logging.basicConfig(level=logging.INFO)

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = os.environ.get('RABBITMQ_AMQP_PORT')
RABBITMQ_USERNAME = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_USER')

# Database connection parameters
DB_HOST = 'db'
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_NAME = 'planning'

def create_database_connection():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return connection
    except Error as e:
        logging.error(f"Error connecting to database: {e}")
        return None

def create_or_update_table(connection):
    try:
        cursor = connection.cursor()

        # Controle: bestaat de tabel 'users'?
        cursor.execute("SHOW TABLES LIKE 'users'")
        table_exists = cursor.fetchone()

        expected_columns = {
            "id": "INT AUTO_INCREMENT PRIMARY KEY",
            "user_id": "VARCHAR(50) UNIQUE NOT NULL",
            "first_name": "VARCHAR(50) NOT NULL",
            "last_name": "VARCHAR(50) NOT NULL",
            "email": "VARCHAR(100) UNIQUE NOT NULL",
            "title": "VARCHAR(20)",
            "password": "VARCHAR(255) NOT NULL",
            "is_admin": "BOOLEAN DEFAULT FALSE"
        }

        if not table_exists:
            # Tabel bestaat nog niet, maak alles in één keer aan
            column_defs = ",\n".join([f"{col} {typ}" for col, typ in expected_columns.items()])
            create_query = f"CREATE TABLE users ({column_defs})"
            cursor.execute(create_query)
            logging.info("Tabel 'users' aangemaakt")
        else:
            # Tabel bestaat: check bestaande kolommen
            cursor.execute("SHOW COLUMNS FROM users")
            existing_columns = [row[0] for row in cursor.fetchall()]
            logging.info(f"Bestaande kolommen in 'users': {existing_columns}")

            for col, col_type in expected_columns.items():
                if col not in existing_columns:
                    logging.warning(f"Kolom '{col}' ontbreekt en wordt toegevoegd...")
                    if col == "user_id":
                        # Speciaal geval: user_id moet UNIQUE NOT NULL zijn
                        cursor.execute("ALTER TABLE users ADD COLUMN user_id VARCHAR(50)")
                        logging.info("Kolom 'user_id' toegevoegd zonder constraints")

                        cursor.execute("DELETE FROM users WHERE user_id IS NULL OR user_id = ''")
                        removed = cursor.rowcount
                        if removed > 0:
                            logging.warning(f"{removed} rijen zonder geldige user_id verwijderd")

                        cursor.execute("ALTER TABLE users MODIFY COLUMN user_id VARCHAR(50) UNIQUE NOT NULL")
                        logging.info("Kolom 'user_id' aangepast naar UNIQUE NOT NULL")
                    else:
                        alter_query = f"ALTER TABLE users ADD COLUMN {col} {col_type}"
                        cursor.execute(alter_query)
                        logging.info(f"Kolom '{col}' toegevoegd met type {col_type}")

        connection.commit()
    except Error as e:
        logging.error(f"Fout bij aanmaken of aanpassen van tabel: {e}")
    finally:
        cursor.close()

def parse_message(message):
    try:
        root = ET.fromstring(message)
        info = root.find('info')
        operation = info.find('operation').text
        user_elem = root.find('user')
        uid = user_elem.find('uid').text
        first_name = user_elem.find('first_name').text
        last_name = user_elem.find('last_name').text
        email = user_elem.find('email').text
        title = user_elem.find('title').text
        password = user_elem.find('password').text
        is_admin_text = user_elem.find('is_admin').text.lower()
        is_admin = is_admin_text == 'true'
        return operation, uid, first_name, last_name, email, title, password, is_admin
    except Exception as e:
        logging.error(f"Error parsing message: {e}")
        return None, None, None, None, None, None, None, None

def user_id_exists(connection, uid):
    try:
        cursor = connection.cursor()
        query = "SELECT COUNT(*) FROM users WHERE user_id = %s"
        cursor.execute(query, (uid,))
        result = cursor.fetchone()
        return result[0] > 0
    except Error as e:
        logging.error(f"Error checking if user ID exists: {e}")
        return False
    finally:
        cursor.close()

def create_user(connection, uid, first_name, last_name, email, title, password, is_admin):
    try:
        if user_id_exists(connection, uid):
            logging.warning(f"User ID {uid} already exists, skipping creation")
            return False

        cursor = connection.cursor()
        query = """
        INSERT INTO users (user_id, first_name, last_name, email, title, password, is_admin)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (uid, first_name, last_name, email, title, password, is_admin))
        connection.commit()
        logging.info(f"User created: {email} with ID: {uid}")
        return True
    except Error as e:
        logging.error(f"Error creating user: {e}")
        return False
    finally:
        cursor.close()

def update_user(connection, uid, first_name, last_name, email, title, password, is_admin):
    try:
        cursor = connection.cursor()
        query = """
        UPDATE users
        SET first_name=%s, last_name=%s, email=%s, title=%s, password=%s, is_admin=%s
        WHERE user_id=%s
        """
        cursor.execute(query, (first_name, last_name, email, title, password, is_admin, uid))
        if cursor.rowcount == 0:
            logging.warning(f"No user found with ID {uid} to update")
        else:
            connection.commit()
            logging.info(f"User updated: {uid}")
        return True
    except Error as e:
        logging.error(f"Error updating user: {e}")
        return False
    finally:
        cursor.close()

def delete_user(connection, uid):
    try:
        cursor = connection.cursor()
        query = "DELETE FROM users WHERE user_id=%s"
        cursor.execute(query, (uid,))
        if cursor.rowcount == 0:
            logging.warning(f"No user found with ID {uid} to delete")
        else:
            connection.commit()
            logging.info(f"User deleted: {uid}")
        return True
    except Error as e:
        logging.error(f"Error deleting user: {e}")
        return False
    finally:
        cursor.close()

def callback(ch, method, properties, body):
    operation, uid, first_name, last_name, email, title, password, is_admin = parse_message(body)
    if operation is None:
        logging.error("Failed to parse message, acknowledging anyway")
        return

    connection_db = create_database_connection()
    if connection_db is None:
        logging.error("Failed to connect to database, acknowledging message")
        return

    # 🔁 Controleer en update de 'users' tabelstructuur
    if connection_db:
        create_or_update_table(connection_db)

    try:
        if operation == 'create':
            create_user(connection_db, uid, first_name, last_name, email, title, password, is_admin)
        elif operation == 'update':
            update_user(connection_db, uid, first_name, last_name, email, title, password, is_admin)
        elif operation == 'delete':
            delete_user(connection_db, uid)
        else:
            logging.error(f"Unknown operation: {operation}")
    except Exception as e:
        logging.error(f"Unexpected error processing message: {e}")
    finally:
        connection_db.close()

def main():
    credentials = pika.PlainCredentials(username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        virtual_host=RABBITMQ_VHOST
    ))
    channel = connection.channel()
    channel.basic_consume(queue='planning.user', on_message_callback=callback, auto_ack=True)
    logging.info("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()
