import psycopg2, os
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'),
    user=os.getenv('DB_USER'), password=(os.getenv('DB_PASSWORD') or '').strip(),
    dbname=(os.getenv('DB_NAME') or '').strip()
)
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='dim_match' ORDER BY ordinal_position")
cols = [r[0] for r in cur.fetchall()]

with open('cols_out.txt', 'w') as f:
    f.write("DIM_MATCH COLS:\n")
    for c in cols:
        f.write(f"  {c}\n")

    # Sample row
    cur.execute("SELECT * FROM dim_match LIMIT 1")
    row = cur.fetchone()
    f.write("\nSAMPLE ROW:\n")
    for c, v in zip(cols, row):
        f.write(f"  {c}: {v}\n")

    # Also check all tables in public schema
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' ORDER BY table_name")
    tables = [r[0] for r in cur.fetchall()]
    f.write(f"\nTABLES: {tables}\n")

conn.close()
print("Done - see cols_out.txt")
