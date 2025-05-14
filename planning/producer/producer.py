import os
import pika
import xml.etree.ElementTree as ET
from datetime import datetime

# --- RabbitMQ config ---------------------------------------------------------
RABBITMQ_HOST      = 'rabbitmq' 
RABBITMQ_PORT      = int(os.getenv("RABBITMQ_AMQP_PORT"))
RABBITMQ_USERNAME  = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD  = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST     = os.getenv("RABBITMQ_USER")

EXCHANGE_NAME = "event"                   # één exchange voor alles
ROUTING_KEYS  = {                         # topic‐routing
    "event": {
        "create": "event.create",
        "update": "event.update",
        "delete": "event.delete"
    },
    "session": {
        "create": "session.create",
        "update": "session.update",
        "delete": "session.delete"
    }
}

# --- verbinding helper -------------------------------------------------------
def _get_channel():
    creds = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=creds
    )
    conn = pika.BlockingConnection(params)
    ch   = conn.channel()
    return conn, ch

# --- XML helpers -------------------------------------------------------------
def _build_info(parent, operation: str):
    info = ET.SubElement(parent, "info")
    ET.SubElement(info, "sender").text     = "planning"
    ET.SubElement(info, "operation").text  = operation
    return info

def _event_to_xml(data: dict, operation: str) -> bytes:
    root = ET.Element("attendify")
    _build_info(root, operation)

    e = ET.SubElement(root, "event")
    ET.SubElement(e, "id").text = data["event_id"]

    if operation != "delete":
        ET.SubElement(e, "uid").text         = data["session_id"]
        ET.SubElement(e, "title").text         = data["title"]
        ET.SubElement(e, "gcal_id").text = data.get("gcal_id", "")
        ET.SubElement(e, "description").text   = data.get("description", "")
        ET.SubElement(e, "location").text      = data.get("location", "")
        ET.SubElement(e, "start_date").text    = str(data["start_date"])
        ET.SubElement(e, "end_date").text      = str(data["end_date"])
        ET.SubElement(e, "start_time").text    = str(data["start_time"])
        ET.SubElement(e, "end_time").text      = str(data["end_time"])
        ET.SubElement(e, "organizer_name").text= data.get("organizer_name", "")
        ET.SubElement(e, "organizer_uid").text = data.get("organizer_uid", "")
        ET.SubElement(e, "entrance_fee").text  = str(data.get("entrance_fee", "0.00"))

    return ET.tostring(root, encoding="utf-8")


def _session_to_xml(data: dict, operation: str) -> bytes:
    root = ET.Element("attendify")
    _build_info(root, operation)

    s = ET.SubElement(root, "session")
    ET.SubElement(s, "id").text = data["session_id"]

    if operation != "delete":
        ET.SubElement(s, "uid").text         = data["session_id"]
        ET.SubElement(s, "event_id").text    = data["event_id"]
        ET.SubElement(s, "gcal_id").text = data.get("gcal_id", "")
        ET.SubElement(s, "title").text       = data["title"]
        ET.SubElement(s, "description").text = data.get("description", "")
        ET.SubElement(s, "date").text        = str(data["date"])
        ET.SubElement(s, "start_time").text  = str(data["start_time"])
        ET.SubElement(s, "end_time").text    = str(data["end_time"])
        ET.SubElement(s, "location").text    = data.get("location", "")
        ET.SubElement(s, "max_attendees").text = str(data.get("max_attendees", 0))

        speaker = ET.SubElement(s, "speaker")
        ET.SubElement(speaker, "name").text = f"{data.get('speaker_first_name','')} {data.get('speaker_name','')}".strip()
        ET.SubElement(speaker, "bio").text  = data.get("speaker_bio", "")

    return ET.tostring(root, encoding="utf-8")


# --- openbare API ------------------------------------------------------------
def publish_event(data: dict, operation: str = "create") -> None:
    if operation not in ROUTING_KEYS["event"]:
        raise ValueError("Invalid operation for event")
    xml_bytes = _event_to_xml(data, operation)
    _publish(xml_bytes, ROUTING_KEYS["event"][operation])

def publish_session(data: dict, operation: str = "create") -> None:
    if operation not in ROUTING_KEYS["session"]:
        raise ValueError("Invalid operation for session")
    xml_bytes = _session_to_xml(data, operation)
    _publish(xml_bytes, ROUTING_KEYS["session"][operation])

def _publish(xml_payload: bytes, routing_key: str):
    # Bepaal exchange op basis van routing key
    exchange = "session" if routing_key.startswith("session.") else "event"

    conn, ch = _get_channel()
    ch.basic_publish(
        exchange=exchange,
        routing_key=routing_key,
        body=xml_payload,
        properties=pika.BasicProperties(content_type="application/xml")
    )
    print(f"📨  Verzonden naar exchange '{exchange}' met key '{routing_key}':\n{xml_payload.decode()}\n")
    conn.close()

