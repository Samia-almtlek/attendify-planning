import pytest
from unittest.mock import patch, MagicMock
import planning.synchronizer.sync as sync
from datetime import datetime

# ---------------------------
# Test hash_row
# ---------------------------

def test_hash_row_ignores_irrelevant_fields():
    row1 = {
        "event_id": "1", "title": "Test", "gcal_id": "abc", "synced": 1,
        "synced_at": datetime.now()
    }
    row2 = {
        "event_id": "1", "title": "Test"
    }
    assert sync.hash_row(row1) == sync.hash_row(row2)

def test_hash_row_normalizes_datetime():
    row = {
        "event_id": "1",
        "title": "Test",
        "start_date": datetime(2024, 1, 1, 10, 0)
    }
    h = sync.hash_row(row)
    assert isinstance(h, str)
    assert len(h) == 64  # SHA-256 hex digest

# ---------------------------
# Test normalize_value
# ---------------------------

def test_normalize_value_none():
    assert sync.normalize_value(None) == ""

def test_normalize_value_datetime():
    dt = datetime(2024, 5, 22, 10, 0)
    assert sync.normalize_value(dt) == dt.isoformat()

def test_normalize_value_other():
    assert sync.normalize_value(123) == "123"
    assert sync.normalize_value("abc") == "abc"

# ---------------------------
# Test build_gcal_payload
# ---------------------------

def test_build_gcal_payload_structure():
    row = {
        "title": "Event",
        "location": "Online",
        "description": "Test",
        "start_date": "2025-01-01",
        "end_date": "2025-01-01",
        "start_time": "10:00",
        "end_time": "11:00"
    }
    payload = sync.build_gcal_payload(row)
    assert payload["summary"] == "Event"
    assert payload["location"] == "Online"
    assert payload["start"]["dateTime"] == "2025-01-01T10:00"
    assert payload["end"]["dateTime"] == "2025-01-01T11:00"

# ---------------------------
# Test build_gcal_payload_session
# ---------------------------

def test_build_gcal_payload_session_structure():
    row = {
        "title": "Session",
        "location": "Room A",
        "description": "Desc",
        "date": "2025-01-01",
        "start_time": "14:00",
        "end_time": "15:00"
    }
    payload = sync.build_gcal_payload_session(row)
    assert payload["summary"] == "Session"
    assert payload["start"]["dateTime"] == "2025-01-01T14:00"
    assert payload["end"]["dateTime"] == "2025-01-01T15:00"

# ---------------------------
# Test get_gcal_id (mocked DB)
# ---------------------------

@patch("planning.synchronizer.sync.mysql.connector.connect")
def test_get_gcal_id_returns_value(mock_connect):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("GCAL123",)
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    result = sync.get_gcal_id(mock_conn, "event_snapshots", "event_id", "EVT1")
    assert result == "GCAL123"

# ---------------------------
# Test remove_from_gcal (mocked service)
# ---------------------------

def test_remove_from_gcal_calls_delete():
    mock_service = MagicMock()
    mock_events = mock_service.events.return_value
    mock_events.delete.return_value.execute.return_value = None

    sync.remove_from_gcal(mock_service, "GCAL123")
    mock_events.delete.assert_called_with(
        calendarId=sync.GCAL_EVENT_CALENDAR_ID,
        eventId="GCAL123"
    )

def test_remove_from_gcal_skips_if_none():
    mock_service = MagicMock()
    sync.remove_from_gcal(mock_service, None)
    mock_service.events().delete.assert_not_called()
