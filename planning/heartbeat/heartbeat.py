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
RABBITMQ_PORT = os.environ.get('RABBITMQ_AMQP_PORT', 5672)  
RABBITMQ_USERNAME = os.environ.get('RABBITMQ_USER', 'attendify')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD', 'uXe5u1oWkh32JyLA')  # Default voor testen
RABBITMQ_VHOST = os.environ.get('RABBITMQ_USER', 'attendify')

# Heartbeat-specific parameters
EXCHANGE_NAME = 'monitoring'
ROUTING_KEY = 'monitoring.heartbeat'


# List of services to monitor (service name + port)
SERVICES = [
    ('planning-db-1', 3306),
    ('planning-phpmyadmin-1', 80),
    ('planning-consumer-1', 80),
    ('planning-consumer-user-link-1', 80),
    ('planning-webforms-1', 80),
    ('planning-synchronizer-db-1', 80)
]

def check_service_status(container_name):
    """Check the status of the container via Docker API"""
    try:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect('/var/run/docker.sock')
        
        endpoint = f'/containers/{container_name}/json'
        request = (
            f"GET {endpoint} HTTP/1.1\r\n"
            f"Host: localhost\r\n"
            f"Connection: close\r\n"
            f"\r\n"
        )

        client.sendall(request.encode())
        client.settimeout(2)

        response = b''
        try:
            while True:
                chunk = client.recv(4096)
                if not chunk:
                    break
                response += chunk
        except socket.timeout:
            pass

        response = response.decode('utf-8')

        parts = response.split('\r\n\r\n', 1)
        if len(parts) != 2:
            logging.error(f"Invalid HTTP response from Docker when checking {container_name}")
            return False
        headers, body = parts

        start = body.find('{')
        end = body.rfind('}') + 1
        json_data = body[start:end]

        container_info = json.loads(json_data)

        if container_info.get('State', {}).get('Status') == 'running':
            return True
        return False

    except Exception as e:
        logging.error(f"Error checking service status for {container_name}: {e}")
        return False
    finally:
        client.close()

def create_heartbeat_message(container_name):
    """Create a heartbeat XML message with container name as sender"""
    info = ET.Element('heartbeat')
    ET.SubElement(info, 'sender').text = 'planning'
    ET.SubElement(info, 'container_name').text = container_name
    ET.SubElement(info, 'timestamp').text = datetime.utcnow().isoformat() + 'Z'
    return ET.tostring(info, encoding='utf-8', method='xml')

def main():
    credentials = pika.PlainCredentials(username=RABBITMQ_USERNAME, password=RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials, virtual_host=RABBITMQ_VHOST)
    )
    channel = connection.channel()

    logging.info(f"Starting heartbeat monitor for services: {[service[0] for service in SERVICES]}")

    try:
        while True:
            sent_heartbeats = []
            down_services = []
            for container_name, port in SERVICES:
                status = check_service_status(container_name)
                if status:
                    message = create_heartbeat_message(container_name)
                    channel.basic_publish(
                        exchange=EXCHANGE_NAME,
                        routing_key=ROUTING_KEY,
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=2)
                    )
                    sent_heartbeats.append(container_name)
                else:
                    down_services.append(container_name)

            time.sleep(1)

    except KeyboardInterrupt:
        logging.info("Heartbeat monitor stopped by user")
    finally:
        connection.close()

if __name__ == "__main__":
    main()
