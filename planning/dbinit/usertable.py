print("Creating the database...")

import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime

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

def generate_custom_id():
    prefix = "PL"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")[:-3]
    return f"{prefix}{timestamp}"

def create_or_update_table(connection):
    try:
        cursor = connection.cursor()

        # Check of tabel bestaat
        cursor.execute("SHOW TABLES LIKE 'users'")
        table_exists = cursor.fetchone()

        expected_columns = {
            "id": "INT AUTO_INCREMENT PRIMARY KEY",
            "user_id": "VARCHAR(50) UNIQUE NOT NULL",
            "first_name": "VARCHAR(50) NOT NULL",
            "last_name": "VARCHAR(50) NOT NULL",
            "email": "VARCHAR(100) UNIQUE NOT NULL",
            "title": "VARCHAR(20)",
            "password": "VARCHAR(255) NOT NULL"  # <-- toegevoegd

        }

        if not table_exists:
            # Tabel aanmaken
            column_defs = ",\n".join([f"{col} {typ}" for col, typ in expected_columns.items()])
            create_query = f"CREATE TABLE users ({column_defs})"
            cursor.execute(create_query)
            print("Tabel 'users' aangemaakt")
        else:
            # Kolommen ophalen
            cursor.execute("SHOW COLUMNS FROM users")
            existing_columns = [row[0] for row in cursor.fetchall()]

            # Voeg ontbrekende kolommen toe
            for col, col_type in expected_columns.items():
                if col not in existing_columns:
                    if col == "user_id":
                        # 1. Voeg user_id toe zonder constraints
                        cursor.execute("ALTER TABLE users ADD COLUMN user_id VARCHAR(50)")
                        print("Kolom 'user_id' toegevoegd zonder constraints")

                        # 2. Verwijder bestaande ongeldige rijen
                        cursor.execute("DELETE FROM users WHERE user_id IS NULL OR user_id = ''")
                        removed = cursor.rowcount
                        if removed > 0:
                            print(f"{removed} gebruiker(s) zonder geldige user_id verwijderd")

                        # 3. Pas nu pas de constraint toe
                        cursor.execute("ALTER TABLE users MODIFY COLUMN user_id VARCHAR(50) UNIQUE NOT NULL")
                        print("Kolom 'user_id' aangepast naar UNIQUE NOT NULL")
                    else:
                        alter_query = f"ALTER TABLE users ADD COLUMN {col} {col_type}"
                        cursor.execute(alter_query)
                        print(f"Kolom '{col}' toegevoegd aan tabel 'users'")

        connection.commit()
    except Error as e:
        print(f"Fout bij aanmaken of aanpassen van tabel: {e}")
    finally:
        cursor.close()

def insert_user(connection):
    try:
        cursor = connection.cursor()
        user_id = generate_custom_id()
        insert_query = """
        INSERT INTO users (user_id, first_name, last_name, email, title)
        VALUES (%s, %s, %s, %s, %s)
        """
        user_data = (user_id, 'Pieter', 'Doe', 'test@test.com', 'mr')
        cursor.execute(insert_query, user_data)
        connection.commit()
        print(f"Gebruiker succesvol toegevoegd met ID: {user_id}")
    except Error as e:
        print(f"Error bij invoegen gebruiker: {e}")
    finally:
        cursor.close()

def main():
    connection = create_connection()
    if connection:
        create_or_update_table(connection)
        # insert_user(connection)  # Optioneel: testgebruiker
        connection.close()
        print("Database verbinding gesloten")

if __name__ == "__main__":
    main()
