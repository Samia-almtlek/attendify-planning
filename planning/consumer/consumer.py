import pika
import mysql.connector
from mysql.connector import Error
import os
import logging
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO)

# RabbitMQ connection parameters
RABBITMQ_HOST = 'planning-rabbit'
RABBITMQ_PORT = 5672
RABBITMQ_USERNAME = 'attendify'
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')  # Default voor testen
RABBITMQ_VHOST = 'attendify'

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

def parse_message(message):
    try:
        root = ET.fromstring(message)
        info = root.find('info')
        operation = info.find('operation').text
        user_elem = root.find('user')
        first_name = user_elem.find('first_name').text
        last_name = user_elem.find('last_name').text
        email = user_elem.find('email').text
        title = user_elem.find('title').text
        # Extra velden zoals 'password' worden genegeerd
        return operation, first_name, last_name, email, title
    except Exception as e:
        logging.error(f"Error parsing message: {e}")
        return None, None, None, None, None

def user_exists(connection, email):
    try:
        cursor = connection.cursor()
        query = "SELECT COUNT(*) FROM users WHERE email = %s"
        cursor.execute(query, (email,))
        result = cursor.fetchone()
        return result[0] > 0  # True als de gebruiker al bestaat
    except Error as e:
        logging.error(f"Error checking if user exists: {e}")
        return False
    finally:
        cursor.close()

def create_user(connection, first_name, last_name, email, title):
    try:
        if user_exists(connection, email):
            logging.warning(f"User with email {email} already exists, skipping creation")
            return False
        cursor = connection.cursor()
        query = "INSERT INTO users (first_name, last_name, email, title) VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (first_name, last_name, email, title))
        connection.commit()
        logging.info(f"User created: {email}")
        return True
    except Error as e:
        logging.error(f"Error creating user: {e}")
        return False

def update_user(connection, first_name, last_name, email, title):
    try:
        cursor = connection.cursor()
        query = "UPDATE users SET first_name=%s, last_name=%s, title=%s WHERE email=%s"
        cursor.execute(query, (first_name, last_name, title, email))
        if cursor.rowcount == 0:
            logging.warning(f"No user found with email {email} to update")
        else:
            connection.commit()
            logging.info(f"User updated: {email}")
    except Error as e:
        logging.error(f"Error updating user: {e}")
    finally:
        cursor.close()

def delete_user(connection, email):
    try:
        cursor = connection.cursor()
        query = "DELETE FROM users WHERE email=%s"
        cursor.execute(query, (email,))
        if cursor.rowcount == 0:
            logging.warning(f"No user found with email {email} to delete")
        else:
            connection.commit()
            logging.info(f"User deleted: {email}")
    except Error as e:
        logging.error(f"Error deleting user: {e}")
    finally:
        cursor.close()

def callback(ch, method, properties, body):
    operation, first_name, last_name, email, title = parse_message(body)
    if operation is None:
        ch.basic_nack(delivery_tag=method.delivery_tag)
        return
    connection_db = create_database_connection()
    if connection_db is None:
        ch.basic_nack(delivery_tag=method.delivery_tag)
        return
    try:
        if operation == 'register':
            success = create_user(connection_db, first_name, last_name, email, title)
            if not success and user_exists(connection_db, email):
                ch.basic_ack(delivery_tag=method.delivery_tag)
            elif success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag)
        elif operation == 'update':
            update_user(connection_db, first_name, last_name, email, title)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        elif operation == 'delete':
            delete_user(connection_db, email)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            logging.error(f"Unknown operation: {operation}")
            ch.basic_nack(delivery_tag=method.delivery_tag)
    except Exception as e:
        logging.error(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag)
    finally:
        connection_db.close()

def main():
    credentials = pika.PlainCredentials(username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials, virtual_host=RABBITMQ_VHOST))
    channel = connection.channel()
    channel.queue_declare(queue='planning.user', durable=True)
    channel.basic_consume(queue='planning.user', on_message_callback=callback)
    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()