from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

print(f"GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")

app = Flask(__name__)

# Auth config
SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Route voor hoofdpagina
@app.route('/')
def home():
    return "Welkom bij de Planning API! Gebruik de /create-event en /grant-access endpoints."

@app.route('/events', methods=['POST'])
def create_event():
    """Maakt een nieuw evenement aan in de Google Calendar."""
    try:
        # Verkrijg de JSON data van de request
        event_data = request.json
        if not event_data:
            return jsonify({"error": "Geen gegevens ontvangen."}), 400

        # Valideer de vereiste velden
        required_fields = ['summary', 'start', 'end']
        for field in required_fields:
            if field not in event_data:
                return jsonify({"error": f"Het veld '{field}' is verplicht."}), 400

        # Verkrijg de Google Calendar service
        service = get_service()

        event = {
            'summary': event_data.get('summary'),
            'start': {
                'dateTime': event_data.get('start'),
                'timeZone': 'Europe/Brussels'
            },
            'end': {
                'dateTime': event_data.get('end'),
                'timeZone': 'Europe/Brussels'
            }
        }

        # Haal de Calendar ID uit de omgevingsvariabelen
        calendar_id = os.getenv('GOOGLE_CALENDAR_ID')
        if not calendar_id:
            return jsonify({"error": "Geen Google Calendar ID opgegeven."}), 400

        # Maak het evenement aan in de Google Calendar
        service.events().insert(calendarId=calendar_id, body=event).execute()

        return jsonify({"status": "Evenement succesvol aangemaakt."}), 201

    except Exception as e:
        return jsonify({"error": f"Er is een fout opgetreden: {str(e)}"}), 500

@app.route('/grant-access', methods=['POST'])
def grant_access():
    """Verleent toegang aan een gebruiker voor de Google Calendar."""
    try:
        service = get_service()

        user_email = os.getenv('USER_EMAIL')
        if not user_email:
            return jsonify({"error": "Geen gebruikers e-mail opgegeven."}), 400

        rule = {
            'scope': {
                'type': 'user',
                'value': user_email
            },
            'role': 'owner'  # Of 'reader' of 'writer', afhankelijk van de toegangsrechten
        }

        # Voeg de toegang toe aan de Google Calendar
        service.acl().insert(calendarId=os.getenv('GOOGLE_CALENDAR_ID'), body=rule).execute()

        return jsonify({"status": f"Toegang verleend aan {user_email}."}), 200

    except Exception as e:
        return jsonify({"error": f"Er is een fout opgetreden: {str(e)}"}), 500

def get_service():
    """Maakt een Google Calendar service object aan met de juiste credentials."""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE, SCOPES
    )
    service = build('calendar', 'v3', credentials=credentials)
    return service

if __name__ == "__main__":
    # Start de Flask server
    app.run(debug=True, host="0.0.0.0", port=5000)
