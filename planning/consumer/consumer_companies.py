import pika
import mysql.connector
from mysql.connector import Error
import os
import logging
import xml.etree.ElementTree as ET
from datetime import datetime

logging.basicConfig(level=logging.INFO)

# RabbitMQ settings
RABBITMQ_HOST = 'rabbitmq'
RABBITMQ_PORT = int(os.environ.get('RABBITMQ_AMQP_PORT'))
RABBITMQ_USERNAME = os.environ.get('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.environ.get('RABBITMQ_PASSWORD')
RABBITMQ_VHOST = os.environ.get('RABBITMQ_USER')

# MySQL settings
DB_HOST = 'db'
DB_USER = 'root'
DB_PASSWORD = 'root'
DB_NAME = 'planning'

def create_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

def ensure_table_exists(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            ondernemingsnummer VARCHAR(20) PRIMARY KEY,
            naam VARCHAR(255),
            btwnummer VARCHAR(20),
            straat VARCHAR(255),
            nummer VARCHAR(10),
            postcode VARCHAR(10),
            gemeente VARCHAR(255),
            facturatie_straat VARCHAR(255),
            facturatie_nummer VARCHAR(10),
            facturatie_postcode VARCHAR(10),
            facturatie_gemeente VARCHAR(255),
            email VARCHAR(255),
            telefoon VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cursor.close()

def parse_company_xml(message):
    try:
        root = ET.fromstring(message)
        bedrijf = root.find('bedrijf')
        data = {
            'ondernemingsnummer': bedrijf.find('ondernemingsNummer').text,
            'naam': bedrijf.find('naam').text,
            'btwnummer': bedrijf.find('btwNummer').text,
            'straat': bedrijf.find('adres/straat').text,
            'nummer': bedrijf.find('adres/nummer').text,
            'postcode': bedrijf.find('adres/postcode').text,
            'gemeente': bedrijf.find('adres/gemeente').text,
            'facturatie_straat': bedrijf.find('facturatieAdres/straat').text,
            'facturatie_nummer': bedrijf.find('facturatieAdres/nummer').text,
            'facturatie_postcode': bedrijf.find('facturatieAdres/postcode').text,
            'facturatie_gemeente': bedrijf.find('facturatieAdres/gemeente').text,
            'email': bedrijf.find('email').text,
            'telefoon': bedrijf.find('telefoon').text
        }
        return data
    except Exception as e:
        logging.error(f"Error parsing XML: {e}")
        return None

def company_exists(conn, ondernemingsnummer):
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM companies WHERE ondernemingsnummer = %s", (ondernemingsnummer,))
    exists = cursor.fetchone()[0] > 0
    cursor.close()
    return exists

def insert_company(conn, data):
    if company_exists(conn, data['ondernemingsnummer']):
        logging.warning(f"Company {data['ondernemingsnummer']} already exists.")
        return

    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO companies (
            ondernemingsnummer, naam, btwnummer, straat, nummer, postcode, gemeente,
            facturatie_straat, facturatie_nummer, facturatie_postcode, facturatie_gemeente,
            email, telefoon
        ) VALUES (%(ondernemingsnummer)s, %(naam)s, %(btwnummer)s, %(straat)s, %(nummer)s, %(postcode)s, %(gemeente)s,
                  %(facturatie_straat)s, %(facturatie_nummer)s, %(facturatie_postcode)s, %(facturatie_gemeente)s,
                  %(email)s, %(telefoon)s)
    """, data)
    conn.commit()
    cursor.close()
    logging.info(f"✅ Company {data['ondernemingsnummer']} created.")

def update_company(conn, data):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE companies SET
            naam = %(naam)s,
            btwnummer = %(btwnummer)s,
            straat = %(straat)s,
            nummer = %(nummer)s,
            postcode = %(postcode)s,
            gemeente = %(gemeente)s,
            facturatie_straat = %(facturatie_straat)s,
            facturatie_nummer = %(facturatie_nummer)s,
            facturatie_postcode = %(facturatie_postcode)s,
            facturatie_gemeente = %(facturatie_gemeente)s,
            email = %(email)s,
            telefoon = %(telefoon)s
        WHERE ondernemingsnummer = %(ondernemingsnummer)s
    """, data)
    conn.commit()
    cursor.close()
    logging.info(f"✏️ Company {data['ondernemingsnummer']} updated.")

def delete_company(conn, ondernemingsnummer):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM companies WHERE ondernemingsnummer = %s", (ondernemingsnummer,))
    conn.commit()
    cursor.close()
    logging.info(f"❌ Company {ondernemingsnummer} deleted.")

def callback(ch, method, properties, body):
    logging.info("📩 Received message")
    conn = create_db_connection()
    ensure_table_exists(conn)

    try:
        operation = properties.type or 'create'  # fallback op 'create' indien geen `type` header
        data = parse_company_xml(body.decode())
        if not data:
            logging.error("❌ Invalid XML structure")
            return

        if operation == 'create':
            insert_company(conn, data)
        elif operation == 'update':
            update_company(conn, data)
        elif operation == 'delete':
            delete_company(conn, data['ondernemingsnummer'])
        else:
            logging.error(f"Unknown operation type: {operation}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        conn.close()
        ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials
    )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='planning.company', durable=True)
    channel.basic_consume(queue='planning.company', on_message_callback=callback)
    logging.info("🟢 Waiting for messages on planning.company queue...")
    channel.start_consuming()

if __name__ == '__main__':
    main()
