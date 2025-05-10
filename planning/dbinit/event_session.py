import mysql.connector
from mysql.connector import Error
import os

def create_connection():
    try:
        return mysql.connector.connect(
            host=os.environ.get('LOCAL_DB_HOST', 'db'),
            user=os.environ.get('LOCAL_DB_USER', 'root'),
            password=os.environ.get('LOCAL_DB_PASSWORD', 'root'),
            database=os.environ.get('LOCAL_DB_NAME', 'planning')
        )
    except Error as e:
        print("Error connecting to MySQL:", e)
        return None

def create_or_update_table(conn, table_name, columns, fks=None):
    cursor = conn.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    if not cursor.fetchone():
        cols_sql = ",\n  ".join(f"{c} {t}" for c,t in columns.items())
        fk_sql = ""
        if fks:
            for fk in fks:
                fk_sql += f",\n  FOREIGN KEY ({fk['column']}) REFERENCES {fk['ref_table']}({fk['ref_column']})"
        sql = f"CREATE TABLE {table_name} (\n  {cols_sql}{fk_sql}\n)"
        cursor.execute(sql)
        print(f"Table '{table_name}' created")
    else:
        cursor.execute(f"SHOW COLUMNS FROM {table_name}")
        existing = {r[0] for r in cursor.fetchall()}
        for c,t in columns.items():
            if c not in existing:
                cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {c} {t}")
                print(f"Added column '{c}' to '{table_name}'")
    conn.commit()
    cursor.close()

def create_event_table(conn):
    cols = {
        "event_id":             "VARCHAR(50) PRIMARY KEY",
        "uid":                  "VARCHAR(50) NOT NULL",
        "title":                "VARCHAR(255) NOT NULL",
        "description":          "TEXT",
        "location":             "VARCHAR(255)",
        "start_date":           "DATE",
        "end_date":             "DATE",
        "start_time":           "TIME",
        "end_time":             "TIME",
        "organizer_uid":        "VARCHAR(50)",
        "organizer_first_name": "VARCHAR(50)",
        "organizer_name":       "VARCHAR(50)",
        "entrance_fee":         "DECIMAL(6,2)",
        "gcal_id":              "VARCHAR(255)",
        "synced":               "TINYINT(1) DEFAULT 0",
        "synced_at":            "TIMESTAMP NULL"
    }
    create_or_update_table(conn, "events", cols)

def create_session_table(conn):
    cols = {
        "session_id":     "VARCHAR(50) PRIMARY KEY",
        "uid":            "VARCHAR(50) NOT NULL",
        "event_id":       "VARCHAR(50) NOT NULL",
        "title":          "VARCHAR(255)",
        "description":    "TEXT",
        "date":           "DATE",
        "start_time":     "TIME",
        "end_time":       "TIME",
        "location":       "VARCHAR(255)",
        "max_attendees":  "INT",
        "speaker_name":   "VARCHAR(255)",
        "speaker_bio":    "TEXT",
        "gcal_id":        "VARCHAR(255)",
        "synced":         "TINYINT(1) DEFAULT 0",
        "synced_at":      "TIMESTAMP NULL"
    }
    fks = [{"column":"event_id","ref_table":"events","ref_column":"event_id"}]
    create_or_update_table(conn, "sessions", cols, fks)

def main():
    conn = create_connection()
    if not conn:
        return
    create_event_table(conn)
    create_session_table(conn)
    conn.close()
    print("Database setup complete")

if __name__ == "__main__":
    main()
