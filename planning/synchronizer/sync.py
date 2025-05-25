import os
import sys
import time
import mysql.connector
import json
import hashlib
import pika
import xml.etree.ElementTree as ET
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

# === RabbitMQ config (producer logging) ===
RABBITMQ_HOST      = 'rabbitmq' 
RABBITMQ_PORT      = int(os.getenv("RABBITMQ_AMQP_PORT", 5672))
RABBITMQ_USERNAME  = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD  = os.getenv("RABBITMQ_PASSWORD")
RABBITMQ_VHOST     = os.getenv("RABBITMQ_USER")

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

def send_monitoring_log(message: str, level: str = "info", sender: str = "sync-service", target="event"):
    """
    target: "event", "session" of "both"
    """
    # Skip logging during unit tests
    if os.getenv("UNITTEST_RUNNING") == "1" or "pytest" in sys.modules or "unittest" in sys.modules:
        return

    log = ET.Element("log")
    ET.SubElement(log, "sender").text = sender
    ET.SubElement(log, "timestamp").text = datetime.utcnow().isoformat() + "Z"
    ET.SubElement(log, "level").text = level
    ET.SubElement(log, "message").text = message
    xml_bytes = ET.tostring(log, encoding="utf-8")

    targets = []
    if target == "both":
        targets = ["event", "session"]
    else:
        targets = [target]

    for exch in targets:
        try:
            conn, ch = _get_channel()
            ch.basic_publish(
                exchange=exch,
                routing_key="monitoring.log",
                body=xml_bytes,
                properties=pika.BasicProperties(content_type="application/xml")
            )
            conn.close()
        except Exception as e:
            print(f"🔴 Failed to send monitoring log to {exch}: {e}")

def log_info(message: str, target="event"):
    print(message)
    send_monitoring_log(message, level="info", target=target)

def log_error(message: str, target="event"):
    print(message)
    send_monitoring_log(message, level="error", target=target)

# === GOOGLE & DB CONFIG ===

DB_CONFIG = {
    'host': os.getenv('LOCAL_DB_HOST', 'db'),
    'user': os.getenv('LOCAL_DB_USER', 'root'),
    'password': os.getenv('LOCAL_DB_PASSWORD', 'root'),
    'database': os.getenv('LOCAL_DB_NAME', 'planning')
}

SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_FILE = 'app/service_account.json'
GCAL_EVENT_CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID')

# --- Producer imports (RabbitMQ event/session messages) ---
sys.path.append('/usr/local/bin')
try:
    from producer.producer import publish_event, publish_session
except ModuleNotFoundError:
    from planning.producer.producer import publish_event, publish_session

# --- HELPERS ---
def get_gcal_service():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('calendar', 'v3', credentials=credentials)

def normalize_value(value):
    if value is None:
        return ""
    if isinstance(value, (datetime, )):
        return value.isoformat()
    return str(value)

def hash_row(row):
    relevant = {
        k: normalize_value(v)
        for k, v in row.items()
        if k not in ('gcal_id', 'synced', 'synced_at')
    }
    json_string = json.dumps(relevant, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(json_string.encode('utf-8')).hexdigest()

def fetch_all(conn, table):
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()
    cur.close()
    return rows

def fetch_snapshot_map(conn, snapshot_table, id_field):
    cur = conn.cursor(dictionary=True)
    cur.execute(f"SELECT {id_field}, content_hash FROM {snapshot_table}")
    result = {row[id_field]: row["content_hash"] for row in cur.fetchall()}
    cur.close()
    return result

def update_snapshot(conn, snapshot_table, id_field, row_id, content_hash, gcal_id):
    cur = conn.cursor()
    cur.execute(f"""
        INSERT INTO {snapshot_table} ({id_field}, content_hash, last_seen, gcal_id)
        VALUES (%s, %s, NOW(), %s)
        ON DUPLICATE KEY UPDATE content_hash = VALUES(content_hash), last_seen = NOW(), gcal_id = VALUES(gcal_id)
    """, (row_id, content_hash, gcal_id))
    conn.commit()
    cur.close()

def delete_from_snapshot(conn, snapshot_table, id_field, row_id):
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {snapshot_table} WHERE {id_field} = %s", (row_id,))
    conn.commit()
    cur.close()

def remove_from_gcal(service, gcal_id):
    log_info(f"🧪 [DEBUG] remove_from_gcal() called with: {gcal_id}", target="both")
    if gcal_id:
        try:
            service.events().delete(calendarId=GCAL_EVENT_CALENDAR_ID, eventId=gcal_id).execute()
            log_info(f"🗑️  GCal event verwijderd: {gcal_id}", target="both")
        except Exception as e:
            log_error(f"⚠️  Verwijderen uit GCal mislukt voor {gcal_id}: {e}", target="both")
    else:
        log_error("⚠️  Geen gcal_id opgegeven, dus niets verwijderd.", target="both")

def get_gcal_id(conn, snapshot_table, id_field, row_id):
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT gcal_id FROM {snapshot_table} WHERE {id_field} = %s", (row_id,))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        log_error(f"⚠️  Fout bij ophalen gcal_id uit snapshot: {e}", target="both")
        return None
    finally:
        cur.close()

# --- SYNC EVENTS ---
def sync_events(service, conn):
    current_rows = fetch_all(conn, "events")
    snapshot_map = fetch_snapshot_map(conn, "event_snapshots", "event_id")
    current_ids = set()

    for row in current_rows:
        eid = row["event_id"]
        current_ids.add(eid)
        current_hash = hash_row(row)
        old_hash = snapshot_map.get(eid)

        if old_hash != current_hash:
            operation = "create" if old_hash is None else "update"
            try:
                if operation == "create":
                    gcal_id = service.events().insert(
                        calendarId=GCAL_EVENT_CALENDAR_ID,
                        body=build_gcal_payload(row)).execute()["id"]
                    mark_synced(conn, "events", "event_id", eid, gcal_id)
                else:
                    gcal_id = row["gcal_id"]
                    service.events().update(
                        calendarId=GCAL_EVENT_CALENDAR_ID,
                        eventId=gcal_id,
                        body=build_gcal_payload(row)).execute()

                log_info(f"✅ Event '{eid}' gesynchroniseerd ({operation})", target="event")
                publish_event(row, operation)
                update_snapshot(conn, "event_snapshots", "event_id", eid, current_hash, gcal_id)
            except Exception as e:
                log_error(f"❌ Event '{eid}' fout bij sync: {e}", target="event")

    # Detect deletes
    for snapshot_id in snapshot_map:
        if snapshot_id not in current_ids:
            log_info(f"🗑️  Event '{snapshot_id}' verwijderd", target="event")
            gcal_id = get_gcal_id(conn, "event_snapshots", "event_id", snapshot_id)
            log_info(f"📎 GCal ID voor delete: {gcal_id}", target="event")
            remove_from_gcal(service, gcal_id)
            publish_event({"event_id": snapshot_id}, operation="delete")
            delete_from_snapshot(conn, "event_snapshots", "event_id", snapshot_id)

def build_gcal_payload(row):
    return {
        "summary": row["title"],
        "location": row["location"],
        "description": row["description"],
        "start": {
            "dateTime": f"{row['start_date']}T{row['start_time']}",
            "timeZone": "Europe/Brussels"
        },
        "end": {
            "dateTime": f"{row['end_date']}T{row['end_time']}",
            "timeZone": "Europe/Brussels"
        }
    }

def mark_synced(conn, table, id_field, row_id, gcal_id):
    cur = conn.cursor()
    cur.execute(f"""
        UPDATE {table}
        SET synced = 1, synced_at = NOW(), gcal_id = %s
        WHERE {id_field} = %s
    """, (gcal_id, row_id))
    conn.commit()
    cur.close()

# --- SYNC SESSIONS ---
def sync_sessions(service, conn):
    current_rows = fetch_all(conn, "sessions")
    snapshot_map = fetch_snapshot_map(conn, "session_snapshots", "session_id")
    current_ids = set()

    for row in current_rows:
        sid = row["session_id"]
        current_ids.add(sid)
        current_hash = hash_row(row)
        old_hash = snapshot_map.get(sid)

        if old_hash != current_hash:
            operation = "create" if old_hash is None else "update"
            try:
                if operation == "create":
                    gcal_id = service.events().insert(
                        calendarId=GCAL_EVENT_CALENDAR_ID,
                        body=build_gcal_payload_session(row)).execute()["id"]
                    mark_synced(conn, "sessions", "session_id", sid, gcal_id)
                else:
                    gcal_id = row["gcal_id"]
                    service.events().update(
                        calendarId=GCAL_EVENT_CALENDAR_ID,
                        eventId=gcal_id,
                        body=build_gcal_payload_session(row)).execute()

                log_info(f"✅ Sessie '{sid}' gesynchroniseerd ({operation})", target="session")
                publish_session(row, operation)
                update_snapshot(conn, "session_snapshots", "session_id", sid, current_hash, gcal_id)

            except Exception as e:
                log_error(f"❌ Sessie '{sid}' fout bij sync: {e}", target="session")

    for snapshot_id in snapshot_map:
        if snapshot_id not in current_ids:
            log_info(f"🗑️  Sessie '{snapshot_id}' verwijderd", target="session")
            gcal_id = get_gcal_id(conn, "session_snapshots", "session_id", snapshot_id)
            log_info(f"📎 GCal ID voor delete: {gcal_id}", target="session")
            remove_from_gcal(service, gcal_id)
            publish_session({"session_id": snapshot_id}, operation="delete")
            delete_from_snapshot(conn, "session_snapshots", "session_id", snapshot_id)

def build_gcal_payload_session(row):
    return {
        "summary": row["title"],
        "location": row["location"],
        "description": row["description"],
        "start": {
            "dateTime": f"{row['date']}T{row['start_time']}",
            "timeZone": "Europe/Brussels"
        },
        "end": {
            "dateTime": f"{row['date']}T{row['end_time']}",
            "timeZone": "Europe/Brussels"
        }
    }

# --- MAIN LOOP ---
def main_loop():
    log_info("🌀 Synchronisator gestart...", target="both")
    service = get_gcal_service()

    while True:
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            log_info("✅ Verbonden met DB", target="both")

            sync_events(service, conn)
            sync_sessions(service, conn)

            conn.close()
        except Exception as e:
            log_error(f"❗ Fout bij verbinding of synchronisatie: {e}", target="both")

        time.sleep(5)

if __name__ == '__main__':
    main_loop()
