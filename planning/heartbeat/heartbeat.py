import pika
import time
import os
import logging
from datetime import datetime
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO)

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = 5672
RABBITMQ_USERNAME = 'attendify'
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'uXe5u1oWkh32JyLA')  # Default voor testen
RABBITMQ_VHOST = 'attendify'

# Heartbeat-specific parameters
SENDER = 'planning'
CONTAINER_NAME = os.environ.get('CONTAINER_NAME', 'heartbeat')  # Kan overschreven worden via env
EXCHANGE_NAME = 'monitoring'
QUEUE_NAME = 'monitoring.heartbeat'
ROUTING_KEY = 'monitoring.heartbeat'

def create_heartbeat_message():
    # Maak XML-structuur
    root = ET.Element('attendify')
    info = ET.SubElement(root, 'info')
    ET.SubElement(info, 'sender').text = SENDER
    ET.SubElement(info, 'container_name').text = CONTAINER_NAME
    ET.SubElement(info, 'timestamp').text = datetime.utcnow().isoformat() + 'Z'  # ISO 8601 met UTC
    return ET.tostring(root, encoding='utf-8', method='xml')

def main():
    credentials = pika.PlainCredentials(username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials, virtual_host=RABBITMQ_VHOST)
    )
    channel = connection.channel()

    # Declareer de exchange (type 'direct' voor eenvoudige routing)
    channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic', durable=True)

    # Declareer de queue en bind deze aan de exchange met de routing key
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_bind(queue=QUEUE_NAME, exchange=EXCHANGE_NAME, routing_key=ROUTING_KEY)

    logging.info(f"Starting heartbeat for {CONTAINER_NAME} to exchange {EXCHANGE_NAME} with routing key {ROUTING_KEY}")
    try:
        while True:
            message = create_heartbeat_message()
            channel.basic_publish(
                exchange=EXCHANGE_NAME,
                routing_key=ROUTING_KEY,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)  # Maakt berichten persistent
            )
            logging.info(f"Sent heartbeat: {message.decode('utf-8')}")
            time.sleep(1)  # Wacht 1 seconde
    except KeyboardInterrupt:
        logging.info("Heartbeat stopped by user")
    finally:
        connection.close()

if __name__ == "__main__":
    main()