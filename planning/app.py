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

# Route for the home page
@app.route('/')
def home():
    return "Welkom bij de Planning API! Gebruik de /create-event, /get-event, /delete-event en /grant-access endpoints."

# Create a new event with optional colorId (default is 5 for yellow)
@app.route('/events', methods=['POST'])
def create_event():
    """Creates a new event in the Google Calendar."""
    try:
        # Get the JSON data from the request
        event_data = request.json
        if not event_data:
            return jsonify({"error": "Geen gegevens ontvangen."}), 400

        # Validate the required fields
        required_fields = ['summary', 'start', 'end']
        for field in required_fields:
            if field not in event_data:
                return jsonify({"error": f"Het veld '{field}' is verplicht."}), 400

        # Set default colorId to 5 (Yellow), but allow the client to specify it
        color_id = event_data.get('colorId', '5')  # Default colorId is 5 (Yellow)

        # Get the Google Calendar service
        service = get_service()

        # Prepare the event details
        event = {
            'summary': event_data.get('summary'),
            'start': {
                'dateTime': event_data.get('start'),
                'timeZone': 'Europe/Brussels'
            },
            'end': {
                'dateTime': event_data.get('end'),
                'timeZone': 'Europe/Brussels'
            },
            'colorId': color_id  # Using the colorId from the request or default to 5 (yellow)
        }

        # Get the Calendar ID from the environment variables
        calendar_id = os.getenv('GOOGLE_CALENDAR_ID')
        if not calendar_id:
            return jsonify({"error": "Geen Google Calendar ID opgegeven."}), 400

        # Create the event in the Google Calendar
        created_event = service.events().insert(calendarId=calendar_id, body=event).execute()

        return jsonify({"status": "Evenement succesvol aangemaakt.", "event_id": created_event['id']}), 201

    except Exception as e:
        return jsonify({"error": f"Er is een fout opgetreden: {str(e)}"}), 500

# List all events in the calendar
@app.route('/events', methods=['GET'])
def list_events():
    """Retrieves a list of all events from Google Calendar."""
    try:
        # Get the Google Calendar service
        service = get_service()

        # List all events in the calendar
        events = service.events().list(calendarId=os.getenv('GOOGLE_CALENDAR_ID')).execute()

        return jsonify(events.get('items', [])), 200

    except Exception as e:
        return jsonify({"error": f"Er is een fout opgetreden bij het ophalen van de evenementen: {str(e)}"}), 500

# Get a specific event from Google Calendar by event_id
@app.route('/get-event/<event_id>', methods=['GET'])
def get_event(event_id):
    """Retrieves details of a specific event from Google Calendar based on event_id."""
    try:
        # Get the Google Calendar service
        service = get_service()

        # Get the event using the Google Calendar API
        event = service.events().get(calendarId=os.getenv('GOOGLE_CALENDAR_ID'), eventId=event_id).execute()

        return jsonify(event), 200

    except Exception as e:
        return jsonify({"error": f"Er is een fout opgetreden bij het ophalen van het evenement: {str(e)}"}), 500

# Delete a specific event from Google Calendar by event_id
@app.route('/delete-event/<event_id>', methods=['DELETE'])
def delete_event(event_id):
    """Deletes a specific event from Google Calendar based on event_id."""
    try:
        # Get the Google Calendar service
        service = get_service()

        # Delete the event using the Google Calendar API
        service.events().delete(calendarId=os.getenv('GOOGLE_CALENDAR_ID'), eventId=event_id).execute()

        return jsonify({"status": f"Evenement met ID {event_id} succesvol verwijderd."}), 200

    except Exception as e:
        return jsonify({"error": f"Er is een fout opgetreden bij het verwijderen van het evenement: {str(e)}"}), 500

# Grant access to a user for the Google Calendar
@app.route('/grant-access', methods=['POST'])
def grant_access():
    """Grants access to a user for the Google Calendar."""
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
            'role': 'owner'  # Or 'reader' or 'writer', depending on the access rights
        }

        # Add access to the Google Calendar
        service.acl().insert(calendarId=os.getenv('GOOGLE_CALENDAR_ID'), body=rule).execute()

        return jsonify({"status": f"Toegang verleend aan {user_email}."}), 200

    except Exception as e:
        return jsonify({"error": f"Er is een fout opgetreden: {str(e)}"}), 500

def get_service():
    """Creates a Google Calendar service object with the appropriate credentials."""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE, SCOPES
    )
    service = build('calendar', 'v3', credentials=credentials)
    return service

if __name__ == "__main__":
    # Start the Flask server
    app.run(debug=True, host="0.0.0.0", port=5000)
