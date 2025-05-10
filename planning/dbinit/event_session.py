import mysql.connector
from mysql.connector import Error
import os

def create_connection():
    try:
        connection = mysql.connector.connect(
            host=os.environ.get('LOCAL_DB_HOST', 'db'),
            user=os.environ.get('LOCAL_DB_USER', 'root'),
            password=os.environ.get('LOCAL_DB_PASSWORD', 'root'),
            database=os.environ.get('LOCAL_DB_NAME', 'planning')
        )
        return connection
    except Error as e:
        print("Verbinding maken met:")
        print("Host:", os.environ.get('LOCAL_DB_HOST'))
        print("User:", os.environ.get('LOCAL_DB_USER'))
        print("Database:", os.environ.get('LOCAL_DB_NAME'))
        print(f"Error bij verbinden met MySQL: {e}")
        return None

def create_or_update_table(connection, table_name, expected_columns, foreign_keys=None):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone()

        if not table_exists:
            # Maak de CREATE TABLE query
            columns_sql = ",\n".join([f"{col} {typ}" for col, typ in expected_columns.items()])
            fk_sql = ""
            if foreign_keys:
                for fk in foreign_keys:
                    fk_sql += f", FOREIGN KEY ({fk['column']}) REFERENCES {fk['ref_table']}({fk['ref_column']})"
            create_query = f"CREATE TABLE {table_name} (\n{columns_sql}{fk_sql}\n)"
            cursor.execute(create_query)
            print(f"Tabel '{table_name}' aangemaakt")
        else:
            # Controleer op ontbrekende kolommen
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            existing_columns = [row[0] for row in cursor.fetchall()]
            for col, col_type in expected_columns.items():
                if col not in existing_columns:
                    alter_query = f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}"
                    cursor.execute(alter_query)
                    print(f"Kolom '{col}' toegevoegd aan '{table_name}'")

        connection.commit()
    except Error as e:
        print(f"Fout bij aanmaken of aanpassen van tabel '{table_name}': {e}")
    finally:
        cursor.close()

def create_event_table(connection):
    expected_columns = {
        "event_id": "VARCHAR(50) PRIMARY KEY",
        "uid": "VARCHAR(50) NOT NULL",
        "title": "VARCHAR(255) NOT NULL",
        "description": "TEXT",
        "location": "VARCHAR(255)",
        "start_date": "DATE",
        "end_date": "DATE",
        "start_time": "TIME",
        "end_time": "TIME",
        "organizer_name": "VARCHAR(255)",
        "organizer_uid": "VARCHAR(50)",
        "entrance_fee": "DECIMAL(6,2)",
        "gcal_id": "VARCHAR(255)",
        "synced": "TINYINT(1) DEFAULT 0",
        "synced_at": "TIMESTAMP NULL"
    }
    create_or_update_table(connection, "events", expected_columns)

def create_session_table(connection):
    expected_columns = {
        "session_id": "VARCHAR(50) PRIMARY KEY",
        "uid": "VARCHAR(50) NOT NULL",
        "event_id": "VARCHAR(50) NOT NULL",
        "title": "VARCHAR(255)",
        "description": "TEXT",
        "date": "DATE",
        "start_time": "TIME",
        "end_time": "TIME",
        "location": "VARCHAR(255)",
        "max_attendees": "INT",
        "speaker_name": "VARCHAR(255)",
        "speaker_bio": "TEXT",
        "gcal_id": "VARCHAR(255)",
        "synced": "TINYINT(1) DEFAULT 0",
        "synced_at": "TIMESTAMP NULL"
    }
    foreign_keys = [{
        "column": "event_id",
        "ref_table": "events",
        "ref_column": "event_id"
    }]
    create_or_update_table(connection, "sessions", expected_columns, foreign_keys)

# ❗️main() is optioneel, gebruik alleen als je dit script apart runt
def main():
    connection = create_connection()
    if connection:
        create_event_table(connection)
        create_session_table(connection)
        connection.close()
        print("Database verbinding gesloten")

if __name__ == "__main__":
    main()
