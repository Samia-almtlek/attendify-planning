import pytest
from flask import session
from unittest.mock import patch, MagicMock
import planning.webforms.webforms as webforms  # jouw Flask app heet zo

@pytest.fixture
def client():
    webforms.app.config["TESTING"] = True
    webforms.app.secret_key = "test"
    with webforms.app.test_client() as client:
        with client.session_transaction() as sess:
            sess['user_email'] = 'test@example.com'
        yield client

# -------------------------------
# LOGIN LOGICA (zonder DB connectie)
# -------------------------------

@patch("planning.webforms.webforms.get_connection")
def test_login_user_not_found(mock_conn, client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_conn.return_value.cursor.return_value = mock_cursor

    response = client.post('/login', data={'email': 'notfound@example.com', 'password': 'secret'})
    assert b"Geen gebruiker gevonden" in response.data

@patch("planning.webforms.webforms.get_connection")
def test_login_not_admin(mock_conn, client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = {'email': 'test@example.com', 'password': 'hashed', 'is_admin': False}
    mock_conn.return_value.cursor.return_value = mock_cursor

    response = client.post('/login', data={'email': 'test@example.com', 'password': 'secret'})
    assert b"geen admin" in response.data.lower()

@patch("planning.webforms.webforms.get_connection")
@patch("planning.webforms.webforms.check_bcrypt_hash")
def test_login_wrong_password(mock_bcrypt, mock_conn, client):
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = {'email': 'test@example.com', 'password': 'hashed', 'is_admin': True}
    mock_conn.return_value.cursor.return_value = mock_cursor
    mock_bcrypt.return_value = False

    response = client.post('/login', data={'email': 'test@example.com', 'password': 'wrong'})
    assert b"Ongeldig wachtwoord" in response.data

# -------------------------------
# EVENT VALIDATIE
# -------------------------------

@patch("planning.webforms.webforms.get_connection")
def test_create_event_missing_field(mock_conn, client):
    response = client.post('/event', data={
        'title': '', 'description': '', 'location': '',
        'start_date': '', 'end_date': '', 'start_time': '', 'end_time': '', 'entrance_fee': ''
    })
    assert b"Alle velden verplicht" in response.data

@patch("planning.webforms.webforms.get_connection")
def test_create_event_invalid_fee(mock_conn, client):
    response = client.post('/event', data={
        'title': 'T', 'description': 'D', 'location': 'L',
        'start_date': '2025-01-01', 'end_date': '2025-01-01',
        'start_time': '10:00', 'end_time': '11:00',
        'entrance_fee': 'abc'
    })
    assert b"Ongeldige prijs" in response.data

@patch("planning.webforms.webforms.get_connection")
def test_create_event_invalid_date(mock_conn, client):
    response = client.post('/event', data={
        'title': 'T', 'description': 'D', 'location': 'L',
        'start_date': '2025-01-02', 'end_date': '2025-01-01',
        'start_time': '10:00', 'end_time': '11:00',
        'entrance_fee': '5.00'
    })
    assert b"Fout in datum/tijd" in response.data

# -------------------------------
# SESSION VALIDATIE
# -------------------------------

@patch("planning.webforms.webforms.get_connection")
def test_create_session_missing_field(mock_conn, client):
    mock_conn.return_value.cursor.return_value.fetchall.return_value = [{"event_id": "E1", "title": "Event"}]
    response = client.post('/session', data={
        'event_id': 'E1', 'title': '', 'description': '', 'date': '',
        'start_time': '', 'end_time': '', 'location': '',
        'max_attendees': '', 'speaker_first_name': '', 'speaker_name': '', 'speaker_bio': ''
    })
    assert b"velden zijn verplicht" in response.data

@patch("planning.webforms.webforms.get_connection")
def test_create_session_invalid_time(mock_conn, client):
    mock_conn.return_value.cursor.return_value.fetchall.return_value = [{"event_id": "E1", "title": "Event"}]
    response = client.post('/session', data={
        'event_id': 'E1', 'title': 'Sessie', 'description': 'D', 'date': '2025-01-01',
        'start_time': '14:00', 'end_time': '13:00', 'location': 'Room',
        'max_attendees': 'abc', 'speaker_first_name': 'A', 'speaker_name': 'B', 'speaker_bio': 'Bio'
    })
    assert b"Ongeldige tijd" in response.data
