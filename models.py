from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Event(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    date = db.Column(db.String(50), nullable=False)
    sessions = db.relationship('Session', backref='event', lazy=True)

class Session(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(100), nullable=False)
    speaker = db.Column(db.String(100))
    event_id = db.Column(db.Integer, db.ForeignKey('event.id'), nullable=False)
