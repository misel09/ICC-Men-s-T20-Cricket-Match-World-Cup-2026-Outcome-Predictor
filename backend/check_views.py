import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    user=os.getenv("DB_USER"),
    password=os.getenv("cricket_passWORD"),
    dbname=os.getenv("DB_NAME")
)
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.views WHERE table_schema='public' ORDER BY table_name")
views = [r[0] for r in cur.fetchall()]
print("VIEWS:", views)

for v in views:
    cur.execute(f"SELECT * FROM {v} LIMIT 0")
    cols = [d[0] for d in cur.description]
    print(f"  {v}: {cols}")
