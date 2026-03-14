from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd

load_dotenv()

def get_conn():
    try:
        return psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
    except:
        return None

conn = get_conn()
if conn:
    df = pd.read_sql("SELECT * FROM vw_phase_analysis LIMIT 1", conn)
    print("COLS:", df.columns.tolist())
    print("SAMPLE:", df.to_dict(orient='records'))
else:
    print("COULD NOT CONNECT")
