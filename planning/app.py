from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import os

app = Flask(__name__)

# Auth voor Google Calendar API
SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', './service-account.json')

def get_service():
    """Haalt een nieuwe service op elke keer dat de route wordt aangeroepen."""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE, SCOPES
    )
    service = build('calendar', 'v3', credentials=credentials)
    return service

@app.route('/create-event', methods=['POST'])
def create_event():
    try:
        event_data = request.json

        # Service opvragen wanneer nodig
        service = get_service()

        event = {
            'summary': event_data.get('summary'),
            'start': {'dateTime': event_data.get('start'), 'timeZone': 'Europe/Brussels'},
            'end': {'dateTime': event_data.get('end'), 'timeZone': 'Europe/Brussels'}
        }

        # Kalender ID kan je ook via een environment variabele laten komen
        calendar_id = os.getenv('GOOGLE_CALENDAR_ID', 'primary')

        service.events().insert(calendarId=calendar_id, body=event).execute()

        return jsonify({"status": "Event created"}), 201

    except Exception as e:
        # Foutafhandeling voor debugging/logging
        return jsonify({"error": str(e)}), 500

@app.route('/grant-access', methods=['POST'])
def grant_access():
    try:
        # E-mailadres ophalen uit JSON of ENV als fallback
        data = request.json
        user_email = data.get('email') or os.getenv('OWNER_EMAIL')

        if not user_email:
            return jsonify({"error": "Geen e-mailadres opgegeven"}), 400

        service = get_service()

        # ACL regel opstellen
        rule = {
            'scope': {
                'type': 'user',
                'value': user_email
            },
            'role': 'owner'  # Of 'writer' als je geen volledige rechten wilt geven
        }

        # Voeg ACL regel toe aan de 'primary' calendar
        created_rule = service.acl().insert(calendarId='primary', body=rule).execute()

        return jsonify({
            "status": f"Toegang verleend aan {user_email}",
            "acl_rule": created_rule
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
