import pytest
import planning.consumer.consumer as consumer
from unittest.mock import MagicMock

def test_parse_message_valid():
    xml = """
    <attendify>
        <info><operation>create</operation></info>
        <user>
            <uid>123</uid>
            <first_name>milad</first_name>
            <last_name>Test</last_name>
            <email>milad@test.com</email>
            <title>Developer</title>
            <password>pass123</password>
            <is_admin>true</is_admin>
        </user>
    </attendify>
    """
    result = consumer.parse_message(xml)
    assert result == ('create', '123', 'milad', 'Test', 'milad@test.com', 'Developer', 'pass123', True)

def test_user_id_exists_true(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = [1]
    exists = consumer.user_id_exists(mock_conn, '123')
    assert exists is True
    mock_cursor.execute.assert_called_once()

def test_user_id_exists_false(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = [0]
    exists = consumer.user_id_exists(mock_conn, '999')
    assert exists is False
