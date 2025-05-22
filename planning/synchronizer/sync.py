import sys
import os
import time
import mysql.connector
import json
import hashlib
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

sys.path.append('/usr/local/bin')
try:
    from producer.producer import publish_event, publish_session
except ModuleNotFoundError:
    from planning.producer.producer import publish_event, publish_session


# -------------------- CONFIG -------------------- #

DB_CONFIG = {
    'host': os.getenv('LOCAL_DB_HOST', 'db'),
    'user': os.getenv('LOCAL_DB_USER', 'root'),
    'password': os.getenv('LOCAL_DB_PASSWORD', 'root'),
    'database': os.getenv('LOCAL_DB_NAME', 'planning')
}

SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_FILE = 'app/service_account.json'
GCAL_EVENT_CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID')

# -------------------- HELPERS -------------------- #

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
    print("🧪 [DEBUG] remove_from_gcal() called with:", gcal_id)
    if gcal_id:
        try:
            service.events().delete(calendarId=GCAL_EVENT_CALENDAR_ID, eventId=gcal_id).execute()
            print(f"🗑️  GCal event verwijderd: {gcal_id}")
        except Exception as e:
            print(f"⚠️  Verwijderen uit GCal mislukt voor {gcal_id}: {e}")
    else:
        print("⚠️  Geen gcal_id opgegeven, dus niets verwijderd.")

def get_gcal_id(conn, snapshot_table, id_field, row_id):
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT gcal_id FROM {snapshot_table} WHERE {id_field} = %s", (row_id,))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"⚠️  Fout bij ophalen gcal_id uit snapshot: {e}")
        return None
    finally:
        cur.close()

# -------------------- SYNC EVENTS -------------------- #

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


                print(f"✅ Event '{eid}' gesynchroniseerd ({operation})")
                publish_event(row, operation)
                update_snapshot(conn, "event_snapshots", "event_id", eid, current_hash, gcal_id)
            except Exception as e:
                print(f"❌ Event '{eid}' fout bij sync: {e}")

    # Detect deletes
    for snapshot_id in snapshot_map:
        if snapshot_id not in current_ids:
            print(f"🗑️  Event '{snapshot_id}' verwijderd")
            gcal_id = get_gcal_id(conn, "event_snapshots", "event_id", snapshot_id)
            print(f"📎 GCal ID voor delete: {gcal_id}")
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


# -------------------- SYNC SESSIONS -------------------- #

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

                print(f"✅ Sessie '{sid}' gesynchroniseerd ({operation})")
                publish_session(row, operation)
                update_snapshot(conn, "session_snapshots", "session_id", sid, current_hash, gcal_id)

            except Exception as e:
                print(f"❌ Sessie '{sid}' fout bij sync: {e}")

    for snapshot_id in snapshot_map:
        if snapshot_id not in current_ids:
            print(f"🗑️  Sessie '{snapshot_id}' verwijderd")
            gcal_id = get_gcal_id(conn, "session_snapshots", "session_id", snapshot_id)
            print(f"📎 GCal ID voor delete: {gcal_id}")
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

# -------------------- MAIN LOOP -------------------- #

def main_loop():
    print("🌀 Synchronisator gestart...")
    service = get_gcal_service()

    while True:
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            print("✅ Verbonden met DB")

            sync_events(service, conn)
            sync_sessions(service, conn)

            conn.close()
        except Exception as e:
            print(f"❗ Fout bij verbinding of synchronisatie: {e}")

        time.sleep(5)

if __name__ == '__main__':
    main_loop()
