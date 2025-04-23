print("Creating the database...")
import mysql.connector
from mysql.connector import Error
import os
from datetime import datetime

def create_connection():
    try:
        connection = mysql.connector.connect(
            host=os.environ.get('LOCAL_DB_HOST', 'db'),  # default fallback 'db'
            user=os.environ.get('LOCAL_DB_USER', 'root'),
            password=os.environ.get('LOCAL_DB_PASSWORD', 'root'),
            database=os.environ.get('LOCAL_DB_NAME', 'planning')
            
        )
        return connection
    except Error as e:
        print(f"Verbinding maken met:")
        print(f"Host:", os.environ.get('LOCAL_DB_HOST'))
        print(f"User:", os.environ.get('LOCAL_DB_USER'))
        print(f"Database:", os.environ.get('LOCAL_DB_NAME'))
        print(f"Error bij verbinden met MySQL: {e}")
        return None

def create_table(connection):
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(50) UNIQUE NOT NULL,
            first_name VARCHAR(50) NOT NULL,
            last_name VARCHAR(50) NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            title VARCHAR(20)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        print("Tabel 'users' succesvol aangemaakt of bestaat al")
    except Error as e:
        print(f"Error bij aanmaken tabel: {e}")
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
        user_data = ('Pieter', 'Doe', 'test@test.com', 'mr')
        cursor.execute(insert_query, user_data)
        connection.commit()
        print(f"Gebruiker succesvol toegevoegd met ID: {user_id}")
    except Error as e:
        print(f"Error bij invoegen gebruiker: {e}")
    finally:
        cursor.close()

def main():
    # Maak verbinding met database
    connection = create_connection()
    
    if connection:
        # Maak de tabel aan
        create_table(connection)
        
        # Voeg de voorbeeldgebruiker toe
        
        # Sluit de verbinding
        connection.close()
        print("Database verbinding gesloten")

if __name__ == "__main__":
    main()