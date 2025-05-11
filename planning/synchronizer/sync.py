import os
import time
import mysql.connector
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

DB_CONFIG = {
    'host': os.getenv('LOCAL_DB_HOST', 'db'),
    'user': os.getenv('LOCAL_DB_USER', 'root'),
    'password': os.getenv('LOCAL_DB_PASSWORD', 'root'),
    'database': os.getenv('LOCAL_DB_NAME', 'planning')
}

SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_FILE = 'app/service_account.json'
GCAL_EVENT_CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID')


def get_gcal_service():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('calendar', 'v3', credentials=credentials)

def fetch_unsynced_events(conn):
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM events WHERE synced = 0")
    rows = cursor.fetchall()
    cursor.close()
    return rows

def mark_as_synced(conn, event_id, gcal_id):
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE events
        SET synced = 1, synced_at = NOW(), gcal_id = %s
        WHERE event_id = %s
    """, (gcal_id, event_id))
    conn.commit()
    cursor.close()

def sync_event_to_gcal(service, event):
    start_dt = f"{event['start_date']}T{str(event['start_time'])}"
    end_dt   = f"{event['end_date']}T{str(event['end_time'])}"


    gcal_event = {
        'summary': event.get('title') or 'Geen titel',
        'location': event.get('location') or '',
        'description': event.get('description') or '',
        'start': {'dateTime': start_dt, 'timeZone': 'Europe/Brussels'},
        'end': {'dateTime': end_dt, 'timeZone': 'Europe/Brussels'}
    }

    import json
    print("📤 Payload naar Google Calendar API:")
    print(json.dumps(gcal_event, indent=2))

    created_event = service.events().insert(calendarId=GCAL_EVENT_CALENDAR_ID, body=gcal_event).execute()
    return created_event.get('id')

def main_loop():
    print("🌀 Synchronisator gestart...")
    service = get_gcal_service()

    while True:
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            print(f"✅ DB connectie gelukt")

            events = fetch_unsynced_events(conn)

            for event in events:
                try:
                    gcal_id = sync_event_to_gcal(service, event)
                    mark_as_synced(conn, event['event_id'], gcal_id)
                    print(f"✅ Event '{event['title']}' gesynchroniseerd naar Google Calendar (ID: {gcal_id})")
                except Exception as e:
                    print(f"❌ Fout bij synchroniseren van event '{event['title']}': {e}")
            conn.close()
        except Exception as e:
            print(f"❗ DB connectie probleem: {e}")

        time.sleep(5)

if __name__ == '__main__':
    main_loop()
