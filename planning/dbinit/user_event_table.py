print("Creating user_event and user_session tables (if needed)...")

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

def create_or_update_link_table(connection, table_name, expected_columns, primary_key):
    try:
        cursor = connection.cursor()

        # Check of tabel bestaat
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone()

        if not table_exists:
            # Tabel aanmaken
            column_defs = ",\n".join([f"{col} {typ}" for col, typ in expected_columns.items()])
            create_query = f"""
            CREATE TABLE {table_name} (
                {column_defs},
                PRIMARY KEY ({primary_key})
            )
            """
            cursor.execute(create_query)
            print(f"Tabel '{table_name}' aangemaakt")
        else:
            # Kolommen ophalen
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            existing_columns = [row[0] for row in cursor.fetchall()]

            # Voeg ontbrekende kolommen toe
            for col, col_type in expected_columns.items():
                if col not in existing_columns:
                    alter_query = f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}"
                    cursor.execute(alter_query)
                    print(f"Kolom '{col}' toegevoegd aan tabel '{table_name}'")

            # Controleer of de juiste primary key er al is (optioneel)
            # Dit is lastig betrouwbaar te checken in MySQL via SQL alleen
            # Als je zeker wil zijn, drop je en hermaak je de constraint

        connection.commit()
    except Error as e:
        print(f"Fout bij aanmaken of aanpassen van tabel '{table_name}': {e}")
    finally:
        cursor.close()

def main():
    connection = create_connection()
    if connection:
        create_or_update_link_table(
            connection,
            "user_event",
            expected_columns={
                "user_id": "VARCHAR(50) NOT NULL",
                "event_id": "VARCHAR(50) NOT NULL",
                "registered_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key="user_id, event_id"
        )

        create_or_update_link_table(
            connection,
            "user_session",
            expected_columns={
                "user_id": "VARCHAR(50) NOT NULL",
                "session_id": "VARCHAR(50) NOT NULL",
                "registered_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
            },
            primary_key="user_id, session_id"
        )

        connection.close()
        print("Database verbinding gesloten")

if __name__ == "__main__":
    main()
