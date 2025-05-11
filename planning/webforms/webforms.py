from flask import Flask, render_template, request
import mysql.connector
import os
import uuid
from datetime import datetime
from flask import redirect, url_for
from mysql.connector.errors import IntegrityError
from flask import session, flash
from functools import wraps
import bcrypt


app = Flask(__name__, template_folder='.')
app.secret_key = 'sdfdssdfsdf'

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
    return redirect(url_for('create_event'))

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_email' not in session:
            flash("Je moet eerst inloggen.")
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def check_bcrypt_hash(stored_hash, input_password):
    return bcrypt.checkpw(input_password.encode(), stored_hash.encode())


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email    = request.form['email']
        password = request.form['password']

        conn = get_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT * FROM users WHERE email = %s", (email,))
        user = cur.fetchone()
        cur.close()
        conn.close()

        if not user:
            return "<p>❌ Geen gebruiker gevonden.</p><a href='/login'>Terug</a>"

        print(f"Hash uit DB: {user['password']}")  # DEBUG

        if check_bcrypt_hash(user['password'], password):
            session['user_email'] = user['email']
            session['user_id'] = user['user_id']
            return redirect(url_for('home'))
        else:
            return "<p>❌ Ongeldig wachtwoord.</p><a href='/login'>Probeer opnieuw</a>"


    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/event', methods=['GET','POST'])
@login_required
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
        eid = f"GC{int(datetime.now().timestamp() * 1000)}"        
        cur = conn.cursor()
        cur.execute("""
          INSERT INTO events (
            event_id, uid, title, description, location,
            start_date, end_date, start_time, end_time,
            organizer_uid, organizer_first_name, organizer_name, entrance_fee
          ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,(
          eid, eid, title, desc, loc,
          sd, ed, st, et,
          org_uid, org_first, org_last, fee
        ))
        conn.commit()
        cur.close()
        conn.close()

        # TODO: XML-log msg hier met org_first + org_last
        return f"<p>✅ Event '{title}' aangemaakt!</p><a href='/'>Home</a>"

    return render_template('event.html')

@app.route('/session', methods=['GET', 'POST'])
@login_required
def create_session():
    conn = get_connection()

    # Haal alle events op voor de dropdown
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT event_id, title FROM events ORDER BY start_date")
    events = cursor.fetchall()
    cursor.close()

    if request.method == 'POST':
        f = request.form
        sid = f"GC{int(datetime.now().timestamp() * 1000)}"        
        uid  = f['event_id']
        eid  = f['event_id']
        title = f['title']
        desc  = f['description']
        date  = f['date']
        stime = f['start_time']
        etime = f['end_time']
        loc   = f['location']
        max_a = f['max_attendees']
        sp_fn = f['speaker_first_name']
        sp_ln = f['speaker_name']
        bio   = f['speaker_bio']

        if not all([eid, title, desc, date, stime, etime, loc, max_a, sp_fn, sp_ln, bio]):
            return "<p>❌ Alle velden zijn verplicht.</p><a href='/session'>Terug</a>"

        try:
            if stime >= etime: raise ValueError()
            max_int = int(max_a)
        except:
            return "<p>❌ Ongeldige tijd of max aantal deelnemers.</p><a href='/session'>Terug</a>"

        cur = conn.cursor()
        cur.execute("""
            INSERT INTO sessions (
                session_id, uid, event_id, title, description,
                date, start_time, end_time, location,
                max_attendees, speaker_first_name, speaker_name, speaker_bio
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            sid, uid, eid, title, desc,
            date, stime, etime, loc,
            max_int, sp_fn, sp_ln, bio
        ))
        conn.commit()
        cur.close()
        conn.close()
        return f"<p>✅ Sessie '{title}' aangemaakt!</p><a href='/'>Terug</a>"

    conn.close()
    return render_template('session.html', events=events)

@app.route('/event/update/<event_id>', methods=['GET', 'POST'])
@login_required
def update_event(event_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    if request.method == 'POST':
        f = request.form
        cur.execute("""
            UPDATE events SET title=%s, description=%s, location=%s,
            start_date=%s, end_date=%s, start_time=%s, end_time=%s,
            entrance_fee=%s WHERE event_id=%s
        """, (f['title'], f['description'], f['location'], f['start_date'],
              f['end_date'], f['start_time'], f['end_time'], f['entrance_fee'], event_id))
        conn.commit()
        cur.close()
        conn.close()
        return redirect(url_for('admin_events'))
    
    cur.execute("SELECT * FROM events WHERE event_id = %s", (event_id,))
    event = cur.fetchone()
    cur.close()
    conn.close()
    return render_template("update_event.html", event=event)

@app.route('/event/delete/<event_id>')
@login_required
def delete_event(event_id):
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM events WHERE event_id = %s", (event_id,))
        conn.commit()
    except IntegrityError:
        # Als er gekoppelde sessies zijn
        cur.close()
        conn.close()
        return "<p>❌ Kan event niet verwijderen: verwijder eerst de bijhorende sessies.</p><a href='/admin/events'>Terug</a>"
    cur.close()
    conn.close()
    return redirect(url_for('admin_events'))

@app.route('/admin/events')
@login_required
def admin_events():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT * FROM events ORDER BY start_date")
    events = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("admin_events.html", events=events)

@app.route('/admin/sessions')
@login_required
def admin_sessions():
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    cur.execute("""
        SELECT s.*, e.title AS event_title
        FROM sessions s
        JOIN events e ON s.event_id = e.event_id
        ORDER BY s.date
    """)
    sessions = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("admin_sessions.html", sessions=sessions)

@app.route('/session/update/<session_id>', methods=['GET', 'POST'])
@login_required
def update_session(session_id):
    conn = get_connection()
    cur = conn.cursor(dictionary=True)
    if request.method == 'POST':
        f = request.form
        cur.execute("""
            UPDATE sessions SET title=%s, description=%s, date=%s, start_time=%s, end_time=%s,
            location=%s, max_attendees=%s, speaker_first_name=%s, speaker_name=%s, speaker_bio=%s
            WHERE session_id=%s
        """, (
            f['title'], f['description'], f['date'], f['start_time'], f['end_time'],
            f['location'], f['max_attendees'], f['speaker_first_name'], f['speaker_name'],
            f['speaker_bio'], session_id
        ))
        conn.commit()
        cur.close()
        conn.close()
        return redirect(url_for('admin_sessions'))

    cur.execute("SELECT * FROM sessions WHERE session_id = %s", (session_id,))
    session = cur.fetchone()
    cur.close()
    conn.close()
    return render_template("update_session.html", session=session)

@app.route('/session/delete/<session_id>')
@login_required
def delete_session(session_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM sessions WHERE session_id = %s", (session_id,))
    conn.commit()
    cur.close()
    conn.close()
    return redirect(url_for('admin_sessions'))

if __name__=='__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
