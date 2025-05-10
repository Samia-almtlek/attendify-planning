from flask import Flask, render_template, request
import mysql.connector
import os
import uuid
from datetime import datetime

app = Flask(__name__, template_folder='.')

DB_CONFIG = {
    'host':     os.getenv('LOCAL_DB_HOST', 'db'),
    'user':     os.getenv('LOCAL_DB_USER', 'root'),
    'password': os.getenv('LOCAL_DB_PASSWORD', 'root'),
    'database': os.getenv('LOCAL_DB_NAME', 'planning')
}

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

def get_user_info_by_email(conn, email):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT user_id, first_name, last_name FROM users WHERE email = %s",
        (email,)
    )
    row = cursor.fetchone()
    cursor.close()
    if row:
        return {
            "uid":        row[0],
            "first_name": row[1],
            "last_name":  row[2]
        }
    return None

@app.route('/')
def home():
    return '''
    <h1>Planning Webforms</h1>
    <ul>
        <li><a href="/event">Event aanmaken</a></li>
        <li><a href="/session">Sessie aanmaken</a></li>
    </ul>
    '''

@app.route('/event', methods=['GET','POST'])
def create_event():
    if request.method == 'POST':
        # 1) form values
        title    = request.form['title']
        descr    = request.form['description']
        loc      = request.form['location']
        start_d  = request.form['start_date']
        end_d    = request.form['end_date']
        start_t  = request.form['start_time']
        end_t    = request.form['end_time']
        org_email= request.form['organizer_email']
        fee_raw  = request.form['entrance_fee']

        # 2) basic validation
        if not all([title,descr,loc,start_d,end_d,start_t,end_t,org_email,fee_raw]):
            return "<p>❌ Alle velden verplicht.</p><a href='/event'>Terug</a>"
        try:
            fee = float(fee_raw)
            if fee < 0: raise ValueError()
        except:
            return "<p>❌ Ongeldige prijs.</p><a href='/event'>Terug</a>"
        try:
            sd = datetime.strptime(start_d,"%Y-%m-%d")
            ed = datetime.strptime(end_d,  "%Y-%m-%d")
            if sd>ed or (sd==ed and start_t>=end_t):
                raise ValueError()
        except:
            return "<p>❌ Fout in datum/tijd.</p><a href='/event'>Terug</a>"

        # 3) organizer lookup
        conn = get_connection()
        info = get_user_info_by_email(conn, org_email)
        if not info:
            conn.close()
            return f"<p>❌ Geen gebruiker met e-mail {org_email}</p><a href='/event'>Terug</a>"

        org_uid  = info['uid']
        org_name = info['last_name']   # we gebruiken enkel deze kolom nu

        # 4) insert event
        event_id = str(uuid.uuid4())
        admin_uid= "admin"
        cur = conn.cursor()
        cur.execute("""
          INSERT INTO events (
            event_id, uid, title, description, location,
            start_date, end_date, start_time, end_time,
            organizer_uid, organizer_name, entrance_fee
          ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
          event_id, admin_uid, title, descr, loc,
          start_d, end_d, start_t, end_t,
          org_uid, org_name, fee
        ))
        conn.commit()
        cur.close()
        conn.close()

        # TODO: XML-log met organizer_name
        return f"<p>✅ Event '{title}' aangemaakt!</p><a href='/'>Home</a>"

    return render_template('event.html')

@app.route('/session', methods=['GET','POST'])
def create_session():
    if request.method == 'POST':
        # … onveranderd …
        pass
    return render_template('session.html')

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
