import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def test_conn():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "168.12.168."),
            port=os.getenv("DB_PORT", "5433"),
            user=os.getenv("DB_USER", "cricket_user"),
            password=os.getenv("cricket_passWORD", ""),
            dbname=os.getenv("DB_NAME", "cricket_db")
        )
        print("Connection successful!")
        cur = conn.cursor()
        cur.execute("SELECT version();")
        print(cur.fetchone())
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_conn()
