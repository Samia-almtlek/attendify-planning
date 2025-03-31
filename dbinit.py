print("Creating the database...")
import mysql.connector
from mysql.connector import Error
import os

def create_connection():
    try:
        connection = mysql.connector.connect(
            host=os.environ.get(os.environ.get('LOCAL_DB_HOST')),
            user=os.environ.get(os.environ.get('LOCAL_DB_USER')),
            password=os.environ.get(os.environ.get('LOCAL_DB_PASSWORD')),
            database=os.environ.get(os.environ.get('LOCAL_DB_NAME'))
        )
        return connection
    except Error as e:
        print(f"Error bij verbinden met MySQL: {e}")
        return None

def create_table(connection):
    try:
        cursor = connection.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id INT AUTO_INCREMENT PRIMARY KEY,
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
        insert_query = """
        INSERT INTO users (first_name, last_name, email, title)
        VALUES (%s, %s, %s, %s)
        """
        user_data = ('Pieter', 'Doe', 'test@test.com', 'mr')
        cursor.execute(insert_query, user_data)
        connection.commit()
        print("Gebruiker succesvol toegevoegd")
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