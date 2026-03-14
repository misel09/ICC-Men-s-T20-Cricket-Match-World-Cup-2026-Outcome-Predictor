"""
ETL Pipeline – ICC T20 Cricket JSON → PostgreSQL
=================================================
Flow:
  1. Discover all .json files in the DATA_DIR folder
  2. Parse each file (info metadata + innings deliveries)
  3. Upsert records into:
       dim_player  → unique players from registry
       dim_match   → one row per match
       fact_delivery → one row per ball
  4. Log success/failure per file
"""

import os
import json
import glob
import logging
from pathlib import Path
from datetime import date

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────
load_dotenv()  # reads .env → DB credentials become env variables

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

DB_URL  = os.getenv("DATABASE_URL", "postgresql://cricket_user:cricket_pass@localhost:5433/cricket_db")
DATA_DIR = os.getenv("DATA_DIR", "./data")   # folder that contains all .json match files


# ── Database helpers ──────────────────────────────────────────────────────────
def get_engine():
    """Create & return an SQLAlchemy engine with a connection pool."""
    engine = create_engine(DB_URL, pool_pre_ping=True)
    log.info("Connected to PostgreSQL.")
    return engine


# ── Parsing helpers ───────────────────────────────────────────────────────────
def parse_players(info: dict) -> pd.DataFrame:
    """
    Extract player registry (UUID → name) from the 'info.registry.people' dict.
    Returns a DataFrame with (player_id, player_name).
    """
    registry = info.get("registry", {}).get("people", {})
    if not registry:
        return pd.DataFrame(columns=["player_id", "player_name"])
    return pd.DataFrame(
        [{"player_id": uid, "player_name": name} for name, uid in registry.items()]
    )


def parse_match(info: dict, filename: str) -> dict:
    """
    Build a single-row dict for dim_match from the JSON 'info' section.
    Handles all edge cases: no result, super over, etc.
    """
    outcome = info.get("outcome", {})
    win_by  = outcome.get("by", {})
    dates   = info.get("dates", [])

    teams = info.get("teams", [])
    team1 = teams[0] if len(teams) > 0 else None
    team2 = teams[1] if len(teams) > 1 else None

    # Player of match may be a list – take the first entry
    potm = info.get("player_of_match", [])
    potm_name = potm[0] if potm else None

    return {
        "match_file":      filename,
        "city":            info.get("city"),
        "venue":           info.get("venue"),
        "match_date":      dates[0] if dates else None,
        "season":          str(info.get("season", "")),
        "team1":           team1,
        "team2":           team2,
        "toss_winner":     info.get("toss", {}).get("winner"),
        "toss_decision":   info.get("toss", {}).get("decision"),
        "winner":          outcome.get("winner"),
        "win_by_runs":     win_by.get("runs"),
        "win_by_wickets":  win_by.get("wickets"),
        "player_of_match": potm_name,
        "balls_per_over":  info.get("balls_per_over", 6),
    }


def parse_deliveries(innings: list, match_id: int, registry: dict) -> pd.DataFrame:
    """
    Flatten the nested innings → overs → deliveries structure into a flat
    DataFrame where each row is one ball.

    registry: {player_name: player_uuid}  (used to map names → UUIDs for FK)
    """
    rows = []
    for inning_idx, inning in enumerate(innings, start=1):
        for over_obj in inning.get("overs", []):
            over_num = over_obj.get("over", 0)
            for ball_idx, delivery in enumerate(over_obj.get("deliveries", []), start=1):
                runs    = delivery.get("runs", {})
                extras  = delivery.get("extras", {})

                # Wicket handling – a delivery may have a "wickets" list
                wicket_list = delivery.get("wickets", [])
                wicket_type = wicket_list[0].get("kind") if wicket_list else None
                player_out_name = None
                if wicket_list:
                    player_out_name = wicket_list[0].get("player_out")

                def to_name(name):
                    """Returns the player name directly instead of mapping to UUID."""
                    return name

                rows.append({
                    "match_id":      match_id,
                    "inning":        inning_idx,
                    "over_number":   over_num,
                    "ball_number":   ball_idx,
                    "batter":        to_name(delivery.get("batter")),
                    "bowler":        to_name(delivery.get("bowler")),
                    "non_striker":   to_name(delivery.get("non_striker")),
                    "runs_batter":   runs.get("batter", 0),
                    "runs_extras":   runs.get("extras", 0),
                    "runs_total":    runs.get("total", 0),
                    "extra_wides":   extras.get("wides", 0),
                    "extra_noballs": extras.get("noballs", 0),
                    "extra_byes":    extras.get("byes", 0),
                    "extra_legbyes": extras.get("legbyes", 0),
                    "wicket_type":   wicket_type,
                    "player_out":    to_name(player_out_name) if player_out_name else None,
                })
    return pd.DataFrame(rows)


# ── Loaders ───────────────────────────────────────────────────────────────────
def load_players(df_players: pd.DataFrame, engine) -> None:
    """Upsert players into dim_player (skip duplicates by UUID)."""
    if df_players.empty:
        return
    for _, row in df_players.iterrows():
        try:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO dim_player (player_id, player_name)
                    VALUES (:pid, :name)
                    ON CONFLICT (player_id) DO UPDATE
                        SET player_name = EXCLUDED.player_name
                """), {"pid": row["player_id"], "name": row["player_name"]})
        except Exception:
            # Two different players can share a name — skip silently
            pass


def load_match(match_row: dict, engine) -> int:
    """
    Insert a match row into dim_match.
    Returns the auto-generated match_id (used as FK for deliveries).
    On re-run, skip if file already loaded (idempotent).
    """
    with engine.begin() as conn:
        existing = conn.execute(text(
            "SELECT match_id FROM dim_match WHERE match_file = :f"
        ), {"f": match_row["match_file"]}).fetchone()
        if existing:
            log.info(f"  [SKIP] Already loaded: {match_row['match_file']}")
            return existing[0]

        result = conn.execute(text("""
            INSERT INTO dim_match (
                match_file, city, venue, match_date, season,
                team1, team2, toss_winner, toss_decision,
                winner, win_by_runs, win_by_wickets, player_of_match, balls_per_over
            ) VALUES (
                :match_file, :city, :venue, :match_date, :season,
                :team1, :team2, :toss_winner, :toss_decision,
                :winner, :win_by_runs, :win_by_wickets, :player_of_match, :balls_per_over
            ) RETURNING match_id
        """), match_row)
        return result.fetchone()[0]


def load_deliveries(df: pd.DataFrame, engine) -> None:
    """Bulk-insert all deliveries for a match using pandas to_sql for speed."""
    if df.empty:
        return
    df.to_sql(
        "fact_delivery",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",   # sends multiple rows per INSERT for performance
        chunksize=500,
    )


# ── Main ETL orchestrator ─────────────────────────────────────────────────────
def run_etl():
    engine  = get_engine()
    files   = glob.glob(os.path.join(DATA_DIR, "**", "*.json"), recursive=True)
    total   = len(files)
    success = 0
    skipped = 0
    failed  = 0

    log.info(f"Found {total} JSON file(s) in '{DATA_DIR}'. Starting ETL...")

    # Fetch valid squad teams to filter files before processing
    try:
        with engine.connect() as conn:
            teams_rows = conn.execute(text("SELECT DISTINCT team FROM dim_squad_2026")).fetchall()
            valid_teams = {row[0] for row in teams_rows}
    except Exception as e:
        log.error(f"Could not load valid teams from dim_squad_2026: {e}")
        return

    log.info(f"Filtering matches strictly to these {len(valid_teams)} squad teams: {', '.join(valid_teams)}")

    for path in files:
        filename = Path(path).name
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            info = data.get("info", {})
            match_teams = info.get("teams", [])
            
            # SKIPPING LOGIC: If a match involves NO team from the squad dataset, ignore it completely
            # Changed from 'all' to 'any' because we want stats for India even when they play an unlisted team
            if not any(t in valid_teams for t in match_teams):
                skipped += 1
                continue

            log.info(f"Processing squad match: {filename}")

            innings    = data.get("innings", [])
            registry   = info.get("registry", {}).get("people", {})  # name → uuid

            # 1. Players
            df_players = parse_players(info)
            load_players(df_players, engine)

            # 2. Match
            match_row  = parse_match(info, filename)
            match_id   = load_match(match_row, engine)

            # 3. Deliveries
            df_deliveries = parse_deliveries(innings, match_id, registry)
            load_deliveries(df_deliveries, engine)

            log.info(f"  ✓ Loaded {len(df_deliveries)} deliveries for match_id={match_id}")
            success += 1

        except Exception as e:
            log.error(f"  ✗ Failed to process {filename}: {e}")
            failed += 1

    log.info(f"\n{'='*50}")
    log.info(f"ETL Complete  →  Success: {success}  |  Skipped: {skipped}  |  Failed: {failed}  |  Total: {total}")


if __name__ == "__main__":
    run_etl()
