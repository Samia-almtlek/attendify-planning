from flask import Flask, render_template, request
import mysql.connector
import os
import uuid
from datetime import datetime

app = Flask(__name__, template_folder='.')

DB_CONFIG = {
    'host':     os.getenv('LOCAL_DB_HOST','db'),
    'user':     os.getenv('LOCAL_DB_USER','root'),
    'password': os.getenv('LOCAL_DB_PASSWORD','root'),
    'database': os.getenv('LOCAL_DB_NAME','planning')
}

def get_connection():
    return mysql.connector.connect(**DB_CONFIG)

def get_user_info_by_email(conn, email):
    cur = conn.cursor()
    cur.execute("SELECT user_id, first_name, last_name FROM users WHERE email=%s", (email,))
    row = cur.fetchone()
    cur.close()
    if row:
        return {'uid': row[0], 'first': row[1], 'last': row[2]}
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
    if request.method=='POST':
        # 1. Lees form
        f = request.form
        title   = f['title']
        desc    = f['description']
        loc     = f['location']
        sd      = f['start_date']
        ed      = f['end_date']
        st      = f['start_time']
        et      = f['end_time']
        email   = f['organizer_email']
        fee_raw = f['entrance_fee']

        # 2. Validatie
        if not all([title,desc,loc,sd,ed,st,et,email,fee_raw]):
            return "<p>❌ Alle velden verplicht.</p><a href='/event'>Terug</a>"
        try:
            fee = float(fee_raw)
            if fee<0: raise ValueError()
        except:
            return "<p>❌ Ongeldige prijs.</p><a href='/event'>Terug</a>"
        try:
            d1 = datetime.strptime(sd,"%Y-%m-%d")
            d2 = datetime.strptime(ed,"%Y-%m-%d")
            if d1>d2 or (d1==d2 and st>=et):
                raise ValueError()
        except:
            return "<p>❌ Fout in datum/tijd.</p><a href='/event'>Terug</a>"

        # 3. Organisator uit DB
        conn = get_connection()
        info = get_user_info_by_email(conn, email)
        if not info:
            conn.close()
            return f"<p>❌ Geen gebruiker met e-mail {email}.</p><a href='/event'>Terug</a>"

        org_uid  = info['uid']
        org_first= info['first']
        org_last = info['last']

        # 4. Insert event
        eid = str(uuid.uuid4())
        cur = conn.cursor()
        cur.execute("""
          INSERT INTO events (
            event_id, uid, title, description, location,
            start_date, end_date, start_time, end_time,
            organizer_uid, organizer_first_name, organizer_name, entrance_fee
          ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,(
          eid, 'admin', title, desc, loc,
          sd, ed, st, et,
          org_uid, org_first, org_last, fee
        ))
        conn.commit()
        cur.close()
        conn.close()

        # TODO: XML-log msg hier met org_first + org_last
        return f"<p>✅ Event '{title}' aangemaakt!</p><a href='/'>Home</a>"

    return render_template('event.html')

@app.route('/session', methods=['GET','POST'])
def create_session():
    if request.method=='POST':
        # je bestaande session-logic…
        pass
    return render_template('session.html')

if __name__=='__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
