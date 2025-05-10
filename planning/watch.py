import requests
import uuid
import os
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv

load_dotenv()

SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
CALENDAR_ID = os.getenv('GOOGLE_CALENDAR_ID')
NGROK_URL = os.getenv('NGROK_FORWARDING_URL')  # bv. https://xxxx.ngrok-free.app

if not NGROK_URL:
    raise ValueError("NGROK_FORWARDING_URL is niet ingesteld in .env")

# Webhook kanaal configuratie
channel_id = str(uuid.uuid4())  # Unieke ID
watch_url = f"{NGROK_URL}/calendar/notify"

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    SERVICE_ACCOUNT_FILE, SCOPES)
service = build('calendar', 'v3', credentials=credentials)

body = {
    'id': channel_id,
    'type': 'web_hook',
    'address': watch_url
}

try:
    response = service.events().watch(calendarId=CALENDAR_ID, body=body).execute()
    print("Webhook registered:")
    print(response)
except Exception as e:
    print(f"Fout bij registreren webhook: {str(e)}")
