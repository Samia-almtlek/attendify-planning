import pika
import time
import os
import logging
from datetime import datetime
import xml.etree.ElementTree as ET
import socket
import json

logging.basicConfig(level=logging.INFO)

# RabbitMQ connection parameters
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = os.environ.get('RABBITMQ_AMQP_PORT')  
RABBITMQ_USERNAME = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_USER')

# Heartbeat-specific parameters
EXCHANGE_NAME = 'event'
ROUTING_KEY_CREATE = 'event.create'
ROUTING_KEY_CANCEL = 'event.cancel'
ROUTING_KEY_UPDATE = 'event.update'

# List of services to monitor (service name + port)


def main():
    credentials = pika.PlainCredentials(username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials, virtual_host=RABBITMQ_VHOST)
    )
    channel = connection.channel()


        connection.close()

if __name__ == "__main__":
    main()
