import psycopg2, os
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'),
    user=os.getenv('DB_USER'), password=(os.getenv('DB_PASSWORD') or '').strip(),
    dbname=(os.getenv('DB_NAME') or '').strip()
)
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='fact_delivery' ORDER BY ordinal_position")
cols = [r[0] for r in cur.fetchall()]
cur.execute("SELECT * FROM fact_delivery LIMIT 2")
rows = cur.fetchall()

with open('delivery_cols.txt', 'w') as f:
    f.write("FACT_DELIVERY COLS:\n")
    for c in cols: f.write(f"  {c}\n")
    f.write("\nSAMPLE ROWS:\n")
    for row in rows:
        for c, v in zip(cols, row):
            f.write(f"  {c}: {v}\n")
        f.write("\n")
    
    # Also check vw_phase_analysis columns
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='vw_phase_analysis' ORDER BY ordinal_position")
    pcols = [r[0] for r in cur.fetchall()]
    f.write(f"\nvw_phase_analysis COLS: {pcols}\n")
    if pcols:
        cur.execute("SELECT * FROM vw_phase_analysis LIMIT 2")
        for row in cur.fetchall():
            for c, v in zip(pcols, row):
                f.write(f"  {c}: {v}\n")
            f.write("\n")

conn.close()
print("Done - see delivery_cols.txt")
