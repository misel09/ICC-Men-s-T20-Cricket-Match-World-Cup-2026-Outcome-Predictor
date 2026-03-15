import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "192.168.12.168"),
        port=os.getenv("DB_PORT", "5433"),
        user=os.getenv("DB_USER", "cricket_user"),
        password=os.getenv("cricket_passWORD", "cricket_pass"),
        dbname=os.getenv("DB_NAME", "cricket_db")
    )

conn = get_conn()
df = pd.read_sql("SELECT DISTINCT team1 FROM dim_match UNION SELECT DISTINCT team2 FROM dim_match", conn)
print("TEAMS:", df.iloc[:,0].tolist())
conn.close()
