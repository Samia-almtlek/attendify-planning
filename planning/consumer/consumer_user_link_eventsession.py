import pika
import mysql.connector
from mysql.connector import Error
import os
import logging
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO)

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_AMQP_PORT', 5672))
RABBITMQ_USERNAME = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_USER')

# Database connection parameters
DB_HOST = 'db'
DB_USER = os.environ.get('LOCAL_DB_USER')
DB_PASSWORD = os.environ.get('LOCAL_DB_PASSWORD')
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
        operation = root.find('info/operation').text.strip()

        if root.find('event_attendee') is not None:
            uid = root.find('event_attendee/uid').text.strip()
            event_id = root.find('event_attendee/event_id').text.strip()
            return 'event', operation, uid, event_id

        elif root.find('session_attendee') is not None:
            uid = root.find('session_attendee/uid').text.strip()
            session_id = root.find('session_attendee/session_id').text.strip()
            return 'session', operation, uid, session_id

        else:
            logging.error("Unknown message type: no event_attendee or session_attendee found.")
            return None, None, None, None

    except Exception as e:
        logging.error(f"Failed to parse XML message: {e}")
        return None, None, None, None

def link_user(connection, entity_type, user_id, entity_id):
    table = "user_event" if entity_type == "event" else "user_session"
    column = "event_id" if entity_type == "event" else "session_id"

    try:
        cursor = connection.cursor()
        query = f"""
        INSERT IGNORE INTO {table} (user_id, {column})
        VALUES (%s, %s)
        """
        cursor.execute(query, (user_id, entity_id))
        connection.commit()
        logging.info(f"Linked user '{user_id}' to {entity_type} '{entity_id}'")
    except Error as e:
        logging.error(f"Error linking user to {entity_type}: {e}")
    finally:
        cursor.close()

def remove_link_user(connection, entity_type, user_id, entity_id):
    table = "user_event" if entity_type == "event" else "user_session"
    column = "event_id" if entity_type == "event" else "session_id"

    try:
        cursor = connection.cursor()
        query = f"""
        DELETE FROM {table} WHERE user_id = %s AND {column} = %s
        """
        cursor.execute(query, (user_id, entity_id))
        connection.commit()
        logging.info(f"Removed link for user '{user_id}' from {entity_type} '{entity_id}'")
    except Error as e:
        logging.error(f"Error removing user from {entity_type}: {e}")
    finally:
        cursor.close()

def callback(ch, method, properties, body):
    message = body.decode()
    entity_type, operation, uid, eid = parse_message(message)

    if not all([entity_type, operation, uid, eid]):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    connection = create_database_connection()
    if not connection:
        logging.error("Could not process message due to DB connection issue.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    if operation == "create":
        link_user(connection, entity_type, uid, eid)
    elif operation == "delete":
        remove_link_user(connection, entity_type, uid, eid)
    else:
        logging.error(f"Unsupported operation: {operation}")

    connection.close()
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    credentials = pika.PlainCredentials(username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_consume(queue='planning.event', on_message_callback=callback)
    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()
