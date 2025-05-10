from flask import Flask, render_template, request
import mysql.connector
import os
import uuid
from datetime import datetime

app = Flask(__name__, template_folder='.')

# DB-configuratie uit environment
DB_CONFIG = {
    'host': os.environ.get('LOCAL_DB_HOST', 'db'),
    'user': os.environ.get('LOCAL_DB_USER', 'root'),
    'password': os.environ.get('LOCAL_DB_PASSWORD', 'root'),
    'database': os.environ.get('LOCAL_DB_NAME', 'planning')
}

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

@app.route('/')
def home():
    return '''
    <h1>Planning Webforms</h1>
    <ul>
        <li><a href="/event">Event aanmaken</a></li>
        <li><a href="/session">Sessie aanmaken</a></li>
    </ul>
    '''

@app.route('/event', methods=['GET', 'POST'])
def create_event():
    if request.method == 'POST':
        title = request.form['title']
        description = request.form['description']
        location = request.form['location']
        start_date = request.form['start_date']
        end_date = request.form['end_date']
        start_time = request.form['start_time']
        end_time = request.form['end_time']
        organizer_name = request.form['organizer_name']
        entrance_fee = request.form['entrance_fee']

        # CHECK 1: empty fields
        if not all([title, description, location, start_date, end_date, start_time, end_time, organizer_name, entrance_fee]):
            return "<p>❌ Error: Alle velden zijn verplicht.</p><a href='/event'>Terug</a>"

        # CHECK 2: entrance fee
        try:
            fee_value = float(entrance_fee)
            if fee_value < 0:
                raise ValueError()
        except ValueError:
            return "<p>❌ Ongeldige toegangsprijs. Moet een positief getal zijn.</p><a href='/event'>Terug</a>"

        # CHECK 3: dates and times
        try:
            s_date = datetime.strptime(start_date, "%Y-%m-%d")
            e_date = datetime.strptime(end_date, "%Y-%m-%d")
            if s_date > e_date:
                return "<p>❌ Startdatum mag niet na de einddatum liggen.</p><a href='/event'>Terug</a>"

            if s_date == e_date and start_time >= end_time:
                return "<p>❌ Op dezelfde dag moet starttijd vóór eindtijd liggen.</p><a href='/event'>Terug</a>"

        except Exception as e:
            return f"<p>❌ Fout in datum/tijd: {e}</p><a href='/event'>Terug</a>"

        # Insert into database
        event_id = str(uuid.uuid4())
        uid = "admin"

        conn = get_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO events (
            event_id, uid, title, description, location,
            start_date, end_date, start_time, end_time,
            organizer_name, entrance_fee
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            event_id, uid, title, description, location,
            start_date, end_date, start_time, end_time,
            organizer_name, fee_value
        ))
        conn.commit()
        cursor.close()
        conn.close()

        return f"<p>✅ Event '{title}' succesvol aangemaakt!</p><a href='/'>Terug naar home</a>"

    return render_template('event.html')

@app.route('/session', methods=['GET', 'POST'])
def create_session():
    if request.method == 'POST':
        session_id = request.form['session_id']
        uid = request.form['uid']
        event_id = request.form['event_id']
        title = request.form['title']
        description = request.form['description']
        date = request.form['date']
        start_time = request.form['start_time']
        end_time = request.form['end_time']
        location = request.form['location']
        max_attendees = request.form['max_attendees']
        speaker_name = request.form['speaker_name']
        speaker_bio = request.form['speaker_bio']

        conn = get_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO sessions (
            session_id, uid, event_id, title, description,
            date, start_time, end_time, location,
            max_attendees, speaker_name, speaker_bio
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            session_id, uid, event_id, title, description,
            date, start_time, end_time, location,
            max_attendees, speaker_name, speaker_bio
        ))
        conn.commit()
        cursor.close()
        conn.close()

        return f"<p>✅ Sessie '{title}' aangemaakt!</p><a href='/'>Terug</a>"

    return render_template('session.html')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
