import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST", "192.168.12.168"),
            port=os.getenv("DB_PORT", "5433"),
            user=os.getenv("DB_USER", "cricket_user"),
            password=os.getenv("cricket_passWORD", "cricket_pass"),
            dbname=os.getenv("DB_NAME", "cricket_db")
        )
    except Exception as e:
        print(f"Error: {e}")
        return None

def check_data():
    conn = get_conn()
    if not conn: return
    
    print("Columns in vw_phase_analysis:")
    df_cols = pd.read_sql("SELECT * FROM vw_phase_analysis LIMIT 0", conn)
    print(df_cols.columns.tolist())
    
    print("\nDistinct teams in vw_phase_analysis:")
    # Assuming team name might be in a different column if 'team' is not there
    # Let's check common columns
    df_teams = pd.read_sql("SELECT DISTINCT batting_team FROM vw_phase_analysis LIMIT 10", conn)
    print(df_teams)
    
    conn.close()

if __name__ == "__main__":
    check_data()
