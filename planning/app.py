from flask import Flask, request
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

app = Flask(__name__)

# Auth voor Google Calendar API
SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_FILE = './service-account.json'

credentials = ServiceAccountCredentials.from_json_keyfile_name(
    SERVICE_ACCOUNT_FILE, SCOPES)
service = build('calendar', 'v3', credentials=credentials)

@app.route('/create-event', methods=['POST'])
def create_event():
    event_data = request.json
    event = {
        'summary': event_data.get('summary'),
        'start': {'dateTime': event_data.get('start'), 'timeZone': 'Europe/Brussels'},
        'end': {'dateTime': event_data.get('end'), 'timeZone': 'Europe/Brussels'}
    }
    service.events().insert(calendarId='primary', body=event).execute()
    return {"status": "Event created"}

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
