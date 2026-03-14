import psycopg2, os
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'), port=os.getenv('DB_PORT'),
    user=os.getenv('DB_USER'), password=(os.getenv('DB_PASSWORD') or '').strip(),
    dbname=(os.getenv('DB_NAME') or '').strip()
)
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='dim_match'")
cols = [r[0] for r in cur.fetchall()]

q = """
SELECT column_name FROM information_schema.columns WHERE table_name='vw_match_summary'
"""
cur.execute(q)
vwcols = [r[0] for r in cur.fetchall()]

with open("cols.txt", "w") as f:
    f.write("dim_match:\n")
    f.write(", ".join(cols))
    f.write("\n\nvw_match_summary:\n")
    f.write(", ".join(vwcols))
conn.close()
