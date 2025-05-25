import os
import sys
import pika
import xml.etree.ElementTree as ET
from datetime import datetime

# --- RabbitMQ config ---------------------------------------------------------
RABBITMQ_HOST      = 'rabbitmq' 
RABBITMQ_PORT      = int(os.getenv("RABBITMQ_AMQP_PORT", 5672))
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

# --- Monitoring log utility --------------------------------------------------
def send_monitoring_log(message: str, level: str = "info", sender: str = "event-producer"):
    # Skip logging during unit tests
    if os.getenv("UNITTEST_RUNNING") == "1" or "pytest" in sys.modules or "unittest" in sys.modules:
        return

    log = ET.Element("log")
    ET.SubElement(log, "sender").text = sender
    ET.SubElement(log, "timestamp").text = datetime.utcnow().isoformat() + "Z"
    ET.SubElement(log, "level").text = level
    ET.SubElement(log, "message").text = message
    xml_bytes = ET.tostring(log, encoding="utf-8")

    try:
        conn, ch = _get_channel()
        ch.basic_publish(
            exchange="user-management",
            routing_key="monitoring.log",
            body=xml_bytes,
            properties=pika.BasicProperties(content_type="application/xml")
        )
        conn.close()
    except Exception as e:
        # Optioneel: log dit lokaal als er echt iets misgaat
        # print(f"🔴 Failed to send monitoring log: {e}")
        pass

def log_info(message: str):
    send_monitoring_log(message, level="info")
    # print(message)  # optioneel voor debug/ontwikkeling

def log_error(message: str):
    send_monitoring_log(message, level="error")
    # print(message)  # optioneel voor debug/ontwikkeling

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
    root = ET.Element("attendify", {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:noNamespaceSchemaLocation": "event.xsd"
    })

    info = ET.SubElement(root, "info")
    ET.SubElement(info, "sender").text = "planning"
    ET.SubElement(info, "operation").text = operation

    e = ET.SubElement(root, "event")
    ET.SubElement(e, "uid").text = data["event_id"]

    if operation != "delete":
        ET.SubElement(e, "gcid").text = data.get("gcal_id", "")
        ET.SubElement(e, "title").text = data["title"]
        ET.SubElement(e, "description").text = data.get("description", "")
        ET.SubElement(e, "location").text = data.get("location", "")
        ET.SubElement(e, "start_date").text = str(data["start_date"])
        ET.SubElement(e, "end_date").text = str(data["end_date"])
        ET.SubElement(e, "start_time").text = str(data["start_time"])
        ET.SubElement(e, "end_time").text = str(data["end_time"])
        ET.SubElement(e, "organizer_name").text = data.get("organizer_name", "")
        ET.SubElement(e, "organizer_uid").text = data.get("organizer_uid", "")
        ET.SubElement(e, "entrance_fee").text = str(data.get("entrance_fee", "0.00"))

    return ET.tostring(root, encoding="utf-8")

def _session_to_xml(data: dict, operation: str) -> bytes:
    root = ET.Element("attendify", {
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:noNamespaceSchemaLocation": "session.xsd"
    })

    info = ET.SubElement(root, "info")
    ET.SubElement(info, "sender").text = "planning"
    ET.SubElement(info, "operation").text = operation

    s = ET.SubElement(root, "session")
    ET.SubElement(s, "uid").text = data["session_id"]

    if operation != "delete":
        ET.SubElement(s, "event_id").text = data["event_id"]
        ET.SubElement(s, "title").text = data["title"]
        ET.SubElement(s, "description").text = data.get("description", "")
        ET.SubElement(s, "date").text = str(data["date"])
        ET.SubElement(s, "start_time").text = str(data["start_time"])
        ET.SubElement(s, "end_time").text = str(data["end_time"])
        ET.SubElement(s, "location").text = data.get("location", "")
        ET.SubElement(s, "max_attendees").text = str(data.get("max_attendees", 0))
        ET.SubElement(s, "gcid").text = data.get("gcal_id", "")

        speaker = ET.SubElement(s, "speaker")
        ET.SubElement(speaker, "name").text = f"{data.get('speaker_first_name','')} {data.get('speaker_name','')}".strip()
        ET.SubElement(speaker, "bio").text = data.get("speaker_bio", "")

    return ET.tostring(root, encoding="utf-8")

# --- openbare API ------------------------------------------------------------
def publish_event(data: dict, operation: str = "create") -> None:
    if operation not in ROUTING_KEYS["event"]:
        raise ValueError("Invalid operation for event")

    # Alleen bij create/update de andere velden verplicht maken
    if operation in ["create", "update"]:
        required_fields = ["event_id", "title", "start_date", "end_date", "start_time", "end_time"]
    else:  # delete
        required_fields = ["event_id"]

    for field in required_fields:
        if field not in data:
            raise KeyError(f"⛔ Required field '{field}' is missing in event data for RabbitMQ publish.")

    try:
        xml_bytes = _event_to_xml(data, operation)
        _publish(xml_bytes, ROUTING_KEYS["event"][operation])
        log_info(f"Event published: {data['event_id']} ({operation})")
    except Exception as e:
        log_error(f"❌ Failed to publish event: {e}")
        raise RuntimeError(f"❌ Failed to publish event to RabbitMQ: {e}")

def publish_session(data: dict, operation: str = "create") -> None:
    if operation not in ROUTING_KEYS["session"]:
        raise ValueError("Invalid operation for session")

    if operation in ["create", "update"]:
        required_fields = ["session_id", "event_id", "title", "date", "start_time", "end_time"]
    else:  # delete
        required_fields = ["session_id"]

    for field in required_fields:
        if field not in data:
            raise KeyError(f"⛔ Required field '{field}' is missing in session data for RabbitMQ publish.")

    try:
        xml_bytes = _session_to_xml(data, operation)
        _publish(xml_bytes, ROUTING_KEYS["session"][operation])
        log_info(f"Session published: {data['session_id']} ({operation})")
    except Exception as e:
        log_error(f"❌ Failed to publish session: {e}")
        raise RuntimeError(f"❌ Failed to publish session to RabbitMQ: {e}")

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
    log_info(f"📨  Verzonden naar exchange '{exchange}' met key '{routing_key}':\n{xml_payload.decode()}\n")
    conn.close()
