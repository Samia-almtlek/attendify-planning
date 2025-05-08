from flask import Flask, request, jsonify
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import os
import pika
import json
from flask_sqlalchemy import SQLAlchemy

# Load environment variables
load_dotenv()

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///planning.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

SCOPES = ['https://www.googleapis.com/auth/calendar']
SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Database models
class Event(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    summary = db.Column(db.String(100))
    start = db.Column(db.String(50))
    end = db.Column(db.String(50))
    sessions = db.relationship('Session', backref='event', cascade="all, delete")

    def serialize(self):
        return {
            'id': self.id,
            'summary': self.summary,
            'start': self.start,
            'end': self.end
        }

class Session(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100))
    speaker = db.Column(db.String(100))
    event_id = db.Column(db.Integer, db.ForeignKey('event.id'))

    def serialize(self):
        return {
            'id': self.id,
            'title': self.title,
            'speaker': self.speaker,
            'event_id': self.event_id
        }

# Google Calendar service
def get_service():
    credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
    return build('calendar', 'v3', credentials=credentials)

# Send to RabbitMQ
def send_to_rabbitmq(event_data):
    rabbit_host = os.getenv('RABBITMQ_HOST')
    rabbit_port = int(os.getenv('RABBITMQ_AMQP_PORT', 5672))
    rabbit_user = os.getenv('RABBITMQ_USER')
    rabbit_pass = os.getenv('RABBITMQ_PASSWORD')

    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    parameters = pika.ConnectionParameters(host=rabbit_host, port=rabbit_port, credentials=credentials, virtual_host='attendify')
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue='planning.event', durable=True)
    message_body = json.dumps(event_data)
    channel.basic_publish(exchange='', routing_key='planning.event', body=message_body)
    connection.close()

# Routes
@app.route('/')
def home():
    return "Planning API draait!"

@app.route('/events', methods=['POST'])
def create_event():
    try:
        data = request.json
        for field in ['summary', 'start', 'end']:
            if field not in data:
                return jsonify({"error": f"{field} is verplicht."}), 400

        service = get_service()
        event = {
            'summary': data['summary'],
            'start': {'dateTime': data['start'], 'timeZone': 'Europe/Brussels'},
            'end': {'dateTime': data['end'], 'timeZone': 'Europe/Brussels'},
            'colorId': data.get('colorId', '5')
        }

        calendar_id = os.getenv('GOOGLE_CALENDAR_ID')
        created_event = service.events().insert(calendarId=calendar_id, body=event).execute()

        send_to_rabbitmq(created_event)
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

@app.route('/events/<string:event_id>', methods=['GET'])
def get_event(event_id):
    try:
        service = get_service()
        event = service.events().get(calendarId=os.getenv('GOOGLE_CALENDAR_ID'), eventId=event_id).execute()
        return jsonify(event), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 404

@app.route('/events/<string:event_id>', methods=['PUT'])
def update_event(event_id):
    try:
        data = request.json
        service = get_service()
        calendar_id = os.getenv('GOOGLE_CALENDAR_ID')
        event = service.events().get(calendarId=calendar_id, eventId=event_id).execute()

        for key in ['summary', 'start', 'end', 'colorId']:
            if key in data:
                if key in ['start', 'end']:
                    event[key] = {'dateTime': data[key], 'timeZone': 'Europe/Brussels'}
                else:
                    event[key] = data[key]

        updated = service.events().update(calendarId=calendar_id, eventId=event_id, body=event).execute()
        send_to_rabbitmq(updated)
        return jsonify({"status": "Event geüpdatet", "event_id": updated['id']}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/events/<string:event_id>', methods=['DELETE'])
def delete_event(event_id):
    try:
        service = get_service()
        service.events().delete(calendarId=os.getenv('GOOGLE_CALENDAR_ID'), eventId=event_id).execute()
        return jsonify({"status": f"Event {event_id} verwijderd."}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 404

@app.route('/calendar/notify', methods=['POST'])
def calendar_notify():
    state = request.headers.get("X-Goog-Resource-State")
    print(f"[Webhook] Triggered. State: {state}")

    if state in ['exists', 'update']:
        try:
            service = get_service()
            events = service.events().list(
                calendarId=os.getenv("GOOGLE_CALENDAR_ID"),
                maxResults=1,
                singleEvents=True,
                orderBy="startTime"
            ).execute()
            items = events.get('items', [])
            if items:
                send_to_rabbitmq(items[0])
        except Exception as e:
            print(f"[Webhook Error] {str(e)}")

    return '', 200

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True, host="0.0.0.0", port=5000)
