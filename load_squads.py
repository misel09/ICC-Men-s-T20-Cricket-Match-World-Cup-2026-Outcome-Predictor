"""
load_squads.py – Load 2026 ICC T20 WC Squad CSV → PostgreSQL dim_squad_2026
============================================================================
Run this ONCE before running etl.py:
    python load_squads.py

CSV format expected:
    team, player_name, role, designation
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

DB_URL      = os.getenv("DATABASE_URL", "postgresql://postgres:miselp0928@localhost:5432/smart_city/cricket_db")
SQUAD_CSV   = os.getenv("SQUAD_CSV", r"E:\hackthon\squads.csv")


def load_squads():
    engine = create_engine(DB_URL, pool_pre_ping=True)

    log.info(f"Reading squad CSV from: {SQUAD_CSV}")
    try:
        df = pd.read_csv(SQUAD_CSV)
    except FileNotFoundError:
        log.error(f"CSV file not found at {SQUAD_CSV}. Please place the file there and try again.")
        return

    # Strip whitespace from all string columns
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)

    # Make column names lowercase and stripped to avoid issues
    df.columns = df.columns.str.strip().str.lower()
    
    # Ensure required columns exist
    for col in ["team", "player_name", "role", "designation"]:
        if col not in df.columns:
            # Maybe the CSV has slightly different casing, try to map
            log.warning(f"Expected column '{col}' not found. Current columns: {list(df.columns)}")

    # Drop rows with no player name
    if "player_name" in df.columns:
        df = df[df["player_name"].notna() & (df["player_name"] != "")]
        df = df.drop_duplicates(subset=["player_name", "team"])

    log.info(f"Found {len(df)} squad players across {df.get('team', pd.Series()).nunique()} teams.")

    inserted = 0
    skipped  = 0

    with engine.begin() as conn:
        for _, row in df.iterrows():
            try:
                conn.execute(text("""
                    INSERT INTO dim_squad_2026 (player_name, team, role, designation)
                    VALUES (:player_name, :team, :role, :designation)
                    ON CONFLICT (player_name, team) DO UPDATE
                        SET role        = EXCLUDED.role,
                            designation = EXCLUDED.designation
                """), {
                    "player_name":   row.get("player_name"),
                    "team":          row.get("team"),
                    "role":          row.get("role") if pd.notna(row.get("role")) else None,
                    "designation":   row.get("designation") if pd.notna(row.get("designation")) else None,
                })
                inserted += 1
            except Exception as e:
                log.warning(f"  Skipping {row.get('player_name')} ({row.get('team')}): {e}")
                skipped += 1

    log.info(f"\n{'='*50}")
    log.info(f"Squad Load Complete: {inserted} inserted/updated  |  {skipped} skipped")

    # Print a summary table of teams and count
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT team, COUNT(*) AS player_count
            FROM dim_squad_2026
            GROUP BY team ORDER BY team
        """))
        print("\n📋 Squad Summary:")
        print(f"  {'Team':<25} {'Players':>8}")
        print(f"  {'-'*33}")
        for row in result:
            print(f"  {row[0]:<25} {row[1]:>8}")


if __name__ == "__main__":
    load_squads()
