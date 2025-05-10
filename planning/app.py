from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import os
import pika
import json

from models import db, Event, Session

# App setup
app = Flask(__name__)

# Database config
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///app.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# Load .env config
load_dotenv()
print(f"GOOGLE_APPLICATION_CREDENTIALS: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")

# Google Calendar auth
SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')


@app.route('/')
def home():
    return "Welkom bij de Planning API! Gebruik de /create-event, /get-event, /delete-event en /grant-access endpoints."


# ---------------- GOOGLE CALENDAR ----------------

@app.route('/events', methods=['POST'])
def create_event():
    try:
        event_data = request.json
        required_fields = ['summary', 'start', 'end']
        for field in required_fields:
            if field not in event_data:
                return jsonify({"error": f"'{field}' is verplicht."}), 400

        color_id = event_data.get('colorId', '5')
        service = get_service()

        event = {
            'summary': event_data['summary'],
            'start': {'dateTime': event_data['start'], 'timeZone': 'Europe/Brussels'},
            'end': {'dateTime': event_data['end'], 'timeZone': 'Europe/Brussels'},
            'colorId': color_id
        }

        calendar_id = os.getenv('GOOGLE_CALENDAR_ID')
        created_event = service.events().insert(calendarId=calendar_id, body=event).execute()
        send_to_rabbitmq(event_data)

        return jsonify({"status": "Event aangemaakt", "event_id": created_event['id']}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/events', methods=['GET'])
def list_events():
    try:
        service = get_service()
        events = service.events().list(calendarId=os.getenv('GOOGLE_CALENDAR_ID')).execute()
        return jsonify(events.get('items', [])), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/get-event/<event_id>', methods=['GET'])
def get_event(event_id):
    try:
        service = get_service()
        event = service.events().get(calendarId=os.getenv('GOOGLE_CALENDAR_ID'), eventId=event_id).execute()
        return jsonify(event), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/delete-event/<event_id>', methods=['DELETE'])
def delete_event(event_id):
    try:
        service = get_service()
        service.events().delete(calendarId=os.getenv('GOOGLE_CALENDAR_ID'), eventId=event_id).execute()
        return jsonify({"status": f"Event {event_id} verwijderd."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/grant-access', methods=['POST'])
def grant_access():
    try:
        service = get_service()
        user_email = os.getenv('USER_EMAIL')
        if not user_email:
            return jsonify({"error": "USER_EMAIL ontbreekt."}), 400

        rule = {
            'scope': {'type': 'user', 'value': user_email},
            'role': 'owner'
        }
        service.acl().insert(calendarId=os.getenv('GOOGLE_CALENDAR_ID'), body=rule).execute()
        return jsonify({"status": f"Toegang verleend aan {user_email}."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_service():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE, SCOPES)
    return build('calendar', 'v3', credentials=credentials)


def send_to_rabbitmq(event_data):
    rabbit_host = os.environ.get('RABBITMQ_HOST')
    rabbit_port = int(os.environ.get('RABBITMQ_AMQP_PORT', 5672))
    rabbit_user = os.environ.get('RABBITMQ_USER')
    rabbit_pass = os.environ.get('RABBITMQ_PASSWORD')

    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=credentials, virtual_host='attendify')
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='planning.event', durable=True)
    message_body = json.dumps(event_data)
    channel.basic_publish(exchange='', routing_key='planning.event', body=message_body)
    connection.close()


# ---------------- LOCAL DATABASE CRUD ----------------

@app.route('/local-events', methods=['POST'])
def create_local_event():
    data = request.get_json()
    new_event = Event(name=data['name'], date=data['date'])
    db.session.add(new_event)
    db.session.commit()
    return jsonify({'message': 'Event toegevoegd', 'id': new_event.id}), 201


@app.route('/local-sessions', methods=['POST'])
def create_local_session():
    data = request.get_json()
    new_session = Session(
        title=data['title'],
        speaker=data['speaker'],
        event_id=data['event_id']
    )
    db.session.add(new_session)
    db.session.commit()
    return jsonify({'message': 'Sessie toegevoegd', 'id': new_session.id}), 201


@app.route('/local-events/<int:event_id>/sessions', methods=['GET'])
def get_sessions_by_event(event_id):
    sessions = Session.query.filter_by(event_id=event_id).all()
    result = [{'id': s.id, 'title': s.title, 'speaker': s.speaker} for s in sessions]
    return jsonify(result), 200


# ---------------- RUN ----------------

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True, host="0.0.0.0", port=5000)
