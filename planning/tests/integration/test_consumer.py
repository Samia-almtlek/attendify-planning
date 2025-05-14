import pika
import mysql.connector
import time

def send_rabbitmq_message():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()
    channel.queue_declare(queue="planning.user")

    message = """
    <root>
        <info><operation>create</operation></info>
        <user>
            <uid>ci-001</uid>
            <first_name>Test</first_name>
            <last_name>User</last_name>
            <email>ci@test.com</email>
            <title>QA Engineer</title>
            <password>testpass</password>
        </user>
    </root>
    """

    channel.basic_publish(
        exchange="",
        routing_key="planning.user",
        body=message.encode("utf-8")
    )
    connection.close()

def test_rabbitmq_to_db_flow():
    send_rabbitmq_message()

    # wacht tot consumer verwerkt heeft
    time.sleep(3)

    conn = mysql.connector.connect(
        host="db",
        user="root",
        password="root",
        database="planning"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE user_id = 'ci-001'")
    result = cursor.fetchone()
    cursor.execute("DELETE FROM users WHERE user_id = 'ci-001'")
    conn.commit()
    conn.close()

    assert result is not None, "User not found in database after message sent"
    assert result[1] == "Test"
    assert result[2] == "User"
