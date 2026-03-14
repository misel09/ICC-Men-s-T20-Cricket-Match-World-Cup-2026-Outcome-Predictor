import psycopg2, os, json
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    host=os.getenv('192.168.12.168'), port=os.getenv('5433'),
    user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD').strip(),
    dbname=os.getenv('DB_NAME').strip()
)
cur = conn.cursor()

cur.execute("SELECT * FROM vw_team_head_to_head LIMIT 0")
h2h_cols = [d[0] for d in cur.description]
print("H2H COLS:", h2h_cols)

cur.execute("SELECT * FROM vw_team_head_to_head LIMIT 1")
row = cur.fetchone()
print("H2H ROW:", dict(zip(h2h_cols, row)))

cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='dim_match' ORDER BY ordinal_position")
dim_cols = [r[0] for r in cur.fetchall()]
print("DIM_MATCH COLS:", dim_cols)

cur.execute("SELECT * FROM dim_match LIMIT 1")
row2 = cur.fetchone()
print("DIM ROW:", dict(zip(dim_cols, row2)))

conn.close()
