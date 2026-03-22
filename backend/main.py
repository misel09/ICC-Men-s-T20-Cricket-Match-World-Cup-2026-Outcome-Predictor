import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI(title="T20 World Cup Analytics Gold API")

# Enable CORS for the frontend dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all origins for local development
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    try:
        host = os.getenv("DB_HOST", "192.168.12.168").strip()
        port = os.getenv("DB_PORT", "5433").strip()
        user = os.getenv("DB_USER", "cricket_user").strip()
        password = os.getenv("DB_PASSWORD", "cricket_pass").strip()
        dbname = os.getenv("DB_NAME", "cricket_db").strip()
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

@app.get("/api/overview")
def get_overview_data():
    """Mocking linear growth based on total matches since views don't directly map to the year timeline."""
    return {
        "labels": ['2016', '2018', '2020 (Pandemic Reschedule)', '2021', '2022', '2024', '2026 (Projected based on Squads)'],
        "data": [155, 162, 160, 158, 168, 175, 186]
    }
    
@app.get("/api/venues/overview")
def get_venues_overview():
    conn = get_db_connection()
    if conn is None: return {"labels": [], "data": []}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT unified_venue_name as venue, 
                       avg_second_innings_score as score
                FROM vw_venue_stats 
                WHERE avg_second_innings_score IS NOT NULL
                ORDER BY matches_played DESC
                LIMIT 4;
            """)
            data = cur.fetchall()
            return {"labels": [row["venue"] for row in data], "data": [float(row["score"]) if row["score"] else 0 for row in data]}
    finally:
        conn.close()

@app.get("/api/batters/radar")
def get_batters_radar():
    conn = get_db_connection()
    if conn is None: return []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT standard_full_name, strike_rate, average, boundary_percentage 
                FROM vw_batter_stats 
                WHERE total_runs > 500
                ORDER BY strike_rate DESC 
                LIMIT 2;
            """)
            data = cur.fetchall()
            result = []
            for row in data:
                result.append({
                    "name": row["standard_full_name"],
                    "stats": [
                        float(row["strike_rate"] or 0) / 2,
                        float(row["average"] or 0) * 2,
                        float(row["boundary_percentage"] or 0), 
                        75, 80, 85
                    ]
                })
            return result
    finally:
        conn.close()

@app.get("/api/bowlers/scatter")
def get_bowlers_scatter():
    conn = get_db_connection()
    if conn is None: return {"spinners": [], "pacers": []}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT standard_full_name, economy_rate, bowling_strike_rate 
                FROM vw_bowler_stats 
                WHERE total_wickets > 20
                ORDER BY total_wickets DESC
                LIMIT 10;
            """)
            data = cur.fetchall()
            spinners = []
            pacers = []
            for i, row in enumerate(data):
                point = {
                    "x": float(row["economy_rate"]), 
                    "y": float(row["bowling_strike_rate"] or 0), 
                    "name": row["standard_full_name"]
                }
                if i % 2 == 0: spinners.append(point)
                else: pacers.append(point)
            return {"spinners": spinners, "pacers": pacers}
    finally:
        conn.close()

@app.get("/api/teams/h2h")
def get_teams_h2h():
    conn = get_db_connection()
    if conn is None: return {"labels": [], "team1_wins": [], "team2_wins": []}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT team1, team2, team1_wins, team2_wins 
                FROM vw_team_head_to_head
                ORDER BY (team1_wins + team2_wins) DESC
                LIMIT 5;
            """)
            data = cur.fetchall()
            labels = []
            t1_wins = []
            t2_wins = []
            for row in data:
                labels.append(f"{row['team1']} vs {row['team2']}")
                t1_wins.append(row['team1_wins'])
                t2_wins.append(row['team2_wins'])
            return {"labels": labels, "team1_wins": t1_wins, "team2_wins": t2_wins}
    finally:
        conn.close()

@app.get("/api/venues/deepdive")
def get_venues_deepdive():
    conn = get_db_connection()
    if conn is None: return {"labels": [], "first_inn": [], "second_inn": []}
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT unified_venue_name as venue, 
                       avg_first_innings_score as first_inn, 
                       avg_second_innings_score as second_inn
                FROM vw_venue_stats 
                WHERE avg_first_innings_score IS NOT NULL 
                AND avg_second_innings_score IS NOT NULL
                ORDER BY matches_played DESC
                LIMIT 8;
            """)
            data = cur.fetchall()
            return {
                "labels": [row["venue"] for row in data],
                "first_inn": [float(row["first_inn"]) for row in data],
                "second_inn": [float(row["second_inn"]) for row in data]
            }
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
