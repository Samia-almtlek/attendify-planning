import pytest
import xml.etree.ElementTree as ET
from unittest.mock import patch
import planning.producer.producer as producer

# ---------------------------
# Fixtures
# ---------------------------

@pytest.fixture
def example_event():
    return {
        "event_id": "EVT123",
        "title": "Test Event",
        "description": "Description",
        "location": "Room A",
        "start_date": "2025-01-01",
        "end_date": "2025-01-01",
        "start_time": "10:00",
        "end_time": "12:00",
        "organizer_name": "Alice",
        "organizer_uid": "USR1",
        "entrance_fee": "10.00",
        "gcal_id": "GC123"
    }

@pytest.fixture
def example_session():
    return {
        "session_id": "SES123",
        "event_id": "EVT123",
        "title": "Session Title",
        "description": "Session Desc",
        "date": "2025-01-02",
        "start_time": "14:00",
        "end_time": "15:00",
        "location": "Room B",
        "max_attendees": 25,
        "gcal_id": "GC456",
        "speaker_first_name": "John",
        "speaker_name": "Doe",
        "speaker_bio": "Speaker bio"
    }

# ---------------------------
# Tests _event_to_xml
# ---------------------------

def test_event_to_xml_contains_all_fields(example_event):
    xml_bytes = producer._event_to_xml(example_event, "create")
    xml = ET.fromstring(xml_bytes)

    assert xml.find(".//uid").text == example_event["event_id"]
    assert xml.find(".//title").text == example_event["title"]
    assert xml.find(".//description").text == example_event["description"]
    assert xml.find(".//organizer_name").text == example_event["organizer_name"]
    assert xml.find(".//entrance_fee").text == example_event["entrance_fee"]

def test_event_to_xml_delete_contains_only_uid():
    xml_bytes = producer._event_to_xml({"event_id": "EVT999"}, "delete")
    xml = ET.fromstring(xml_bytes)
    assert xml.find(".//uid").text == "EVT999"
    assert xml.find(".//title") is None

# ---------------------------
# Tests _session_to_xml
# ---------------------------

def test_session_to_xml_contains_all_fields(example_session):
    xml_bytes = producer._session_to_xml(example_session, "create")
    xml = ET.fromstring(xml_bytes)

    assert xml.find(".//uid").text == example_session["session_id"]
    assert xml.find(".//event_id").text == example_session["event_id"]
    assert xml.find(".//title").text == example_session["title"]
    assert xml.find(".//speaker/name").text.strip() == "John Doe"

def test_session_to_xml_delete_contains_only_uid():
    xml_bytes = producer._session_to_xml({"session_id": "SES999"}, "delete")
    xml = ET.fromstring(xml_bytes)
    assert xml.find(".//uid").text == "SES999"
    assert xml.find(".//title") is None

# ---------------------------
# Tests publish_event / session: required fields en validatie
# ---------------------------

def test_publish_event_missing_field():
    with pytest.raises(KeyError):
        producer.publish_event({"title": "No ID"}, operation="create")

def test_publish_session_invalid_operation():
    with pytest.raises(ValueError):
        producer.publish_session({}, operation="invalid")

def test_publish_event_invalid_operation():
    with pytest.raises(ValueError):
        producer.publish_event({}, operation="notreal")

# ---------------------------
# Mocked publish
# ---------------------------

@patch("planning.producer.producer._publish")
def test_publish_event_triggers_publish(mock_pub, example_event):
    producer.publish_event(example_event, "create")
    mock_pub.assert_called_once()
    args, _ = mock_pub.call_args
    assert b"<event>" in args[0]
    assert args[1] == "event.create"

@patch("planning.producer.producer._publish")
def test_publish_session_triggers_publish(mock_pub, example_session):
    producer.publish_session(example_session, "create")
    mock_pub.assert_called_once()
    args, _ = mock_pub.call_args
    assert b"<session>" in args[0]
    assert args[1] == "session.create"

@patch("planning.producer.producer._publish")
def test_publish_event_delete_triggers_publish(mock_pub):
    data = {"event_id": "EVT999"}
    producer.publish_event(data, "delete")
    mock_pub.assert_called_once()
    args, _ = mock_pub.call_args
    assert b"<event>" in args[0]
    assert b"<uid>EVT999</uid>" in args[0]
    assert args[1] == "event.delete"

@patch("planning.producer.producer._publish")
def test_publish_session_delete_triggers_publish(mock_pub):
    data = {"session_id": "SES999"}
    producer.publish_session(data, "delete")
    mock_pub.assert_called_once()
    args, _ = mock_pub.call_args
    assert b"<session>" in args[0]
    assert b"<uid>SES999</uid>" in args[0]
    assert args[1] == "session.delete"
