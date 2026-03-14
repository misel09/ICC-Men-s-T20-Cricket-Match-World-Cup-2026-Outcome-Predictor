"""
match_names.py – Team-aware smart name matching: Cricsheet → squad full names
==============================================================================
Problem:  Cricsheet stores "V Kohli" for India  |  Squad CSV has "Virat Kohli" in India
Strategy: THREE-PASS matching (all team-validated)
  Pass 1 – Last name exact + first initial + same team played  (highest confidence)
  Pass 2 – Last name exact + first initial  (no team check)
  Pass 3 – Fuzzy fallback (WRatio scorer)

Run after load_squads.py and etl.py:
    python match_names.py
"""

import os
import re
import logging
from collections import defaultdict
from rapidfuzz import process, fuzz
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_URL = os.getenv("DATABASE_URL", "postgresql://cricket_user:cricket_pass@localhost:5433/cricket_db")
FUZZY_THRESHOLD = 70


# ── Name parsing helpers ──────────────────────────────────────────────────────

def normalize(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip().lower())

def last_name(name: str) -> str:
    parts = normalize(name).split()
    return parts[-1] if parts else ""

def first_initial(name: str) -> str:
    parts = normalize(name).split()
    return parts[0][0] if parts else ""

def first_word(name: str) -> str:
    parts = normalize(name).split()
    return parts[0] if parts else ""


# ── Load from DB ──────────────────────────────────────────────────────────────

def load_data(engine):
    with engine.connect() as conn:
        # Squad players WITH their team
        squad_rows = conn.execute(text(
            "SELECT player_name, team FROM dim_squad_2026"
        )).fetchall()

        # Cricsheet players
        cs_rows = conn.execute(text(
            "SELECT player_name FROM dim_player"
        )).fetchall()
        cricsheet_names = [r[0] for r in cs_rows]

        # Build cricsheet player → set of teams they have played FOR
        # Infer from: batter in inning 1 → team1, inning 2 → team2
        cs_teams_rows = conn.execute(text("""
            SELECT DISTINCT f.batter,
                   CASE WHEN f.inning = 1 THEN m.team1 ELSE m.team2 END AS team
            FROM fact_delivery f
            JOIN dim_match m ON f.match_id = m.match_id
            WHERE m.team1 IS NOT NULL AND m.team2 IS NOT NULL AND f.batter IS NOT NULL
        """)).fetchall()

    # {cricsheet_player_name: set(teams)}
    cs_player_teams: dict[str, set] = defaultdict(set)
    for cs_name, team in cs_teams_rows:
        cs_player_teams[cs_name].add(team)

    return squad_rows, cricsheet_names, cs_player_teams


# ── Matching logic ────────────────────────────────────────────────────────────

def build_lastname_index(names):
    """last_name → [full names]"""
    idx = defaultdict(list)
    for n in names:
        idx[last_name(n)].append(n)
    return idx


def pass1_team_match(squad_name, squad_team, cs_index, cs_player_teams):
    """Last name + initial + team confirmation."""
    sq_last = last_name(squad_name)
    sq_init = first_initial(squad_name)
    candidates = cs_index.get(sq_last, [])

    for cs_name in candidates:
        cs_first = first_word(cs_name)
        init_ok = (cs_first == sq_init) or cs_first.startswith(sq_init)
        if not init_ok:
            continue
        teams_played = cs_player_teams.get(cs_name, set())
        if squad_team in teams_played:
            return cs_name, 100   # Name + team confirmed

    return None, 0


def pass2_name_only_match(squad_name, cs_index):
    """Last name + initial only — no team check."""
    sq_last = last_name(squad_name)
    sq_init = first_initial(squad_name)
    candidates = cs_index.get(sq_last, [])

    init_hits = [c for c in candidates if first_word(c).startswith(sq_init)]
    if init_hits:
        return init_hits[0], 90
    if len(candidates) == 1:
        return candidates[0], 75
    return None, 0


def pass3_fuzzy(squad_name, cricsheet_names):
    """WRatio fuzzy fallback."""
    result = process.extractOne(
        squad_name, cricsheet_names,
        scorer=fuzz.WRatio,
        score_cutoff=FUZZY_THRESHOLD,
    )
    if result:
        return result[0], int(result[1])
    return None, 0


# ── Ensure table supports team column ────────────────────────────────────────

def setup_table(engine):
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS player_name_map (
                id                    SERIAL PRIMARY KEY,
                squad_player_name     TEXT NOT NULL,
                squad_team            TEXT NOT NULL,
                cricsheet_player_name TEXT,
                match_score           INT,
                verified              BOOLEAN DEFAULT FALSE,
                UNIQUE (squad_player_name, squad_team)
            )
        """))
        # Add squad_team column if upgrading from old schema
        try:
            conn.execute(text(
                "ALTER TABLE player_name_map ADD COLUMN IF NOT EXISTS squad_team TEXT NOT NULL DEFAULT ''"
            ))
        except Exception:
            pass
    log.info("player_name_map table ready.")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_matching():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    setup_table(engine)

    squad_rows, cricsheet_names, cs_player_teams = load_data(engine)
    cs_index = build_lastname_index(cricsheet_names)

    log.info(f"Cricsheet players:   {len(cricsheet_names)}")
    log.info(f"Squad players:       {len(squad_rows)}")
    log.info(f"CS team mappings:    {sum(len(v) for v in cs_player_teams.values())} entries")

    p1 = p2 = p3 = unmatched = 0
    low_conf = []
    results = []

    for squad_name, squad_team in squad_rows:
        # Pass 1 – last name + initial + team
        cs_name, score = pass1_team_match(squad_name, squad_team, cs_index, cs_player_teams)
        if cs_name:
            p1 += 1
        else:
            # Pass 2 – last name + initial only
            cs_name, score = pass2_name_only_match(squad_name, cs_index)
            if cs_name:
                p2 += 1
            else:
                # Pass 3 – fuzzy
                cs_name, score = pass3_fuzzy(squad_name, cricsheet_names)
                if cs_name:
                    p3 += 1
                else:
                    unmatched += 1

        if cs_name and score < 85:
            low_conf.append((squad_name, squad_team, cs_name, score))

        results.append({"squad": squad_name, "team": squad_team,
                         "cricsheet": cs_name, "score": score})

    # Write mappings
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM player_name_map"))
        for r in results:
            conn.execute(text("""
                INSERT INTO player_name_map
                    (squad_player_name, squad_team, cricsheet_player_name, match_score)
                VALUES (:squad, :team, :cricsheet, :score)
                ON CONFLICT (squad_player_name, squad_team)
                DO UPDATE SET cricsheet_player_name = EXCLUDED.cricsheet_player_name,
                              match_score           = EXCLUDED.match_score,
                              verified              = FALSE
            """), r)

    # ── Summary ──────────────────────────────────────────────────────────────
    total = len(squad_rows)
    log.info(f"\n{'='*65}")
    log.info(f"Matching Complete ({total} squad players):")
    log.info(f"  Pass 1 (name + team):   {p1:>4} matched")
    log.info(f"  Pass 2 (name only):     {p2:>4} matched")
    log.info(f"  Pass 3 (fuzzy):         {p3:>4} matched")
    log.info(f"  No match:               {unmatched:>4}")

    print(f"\n📋 Sample mappings (first 20):")
    print(f"  {'Squad Name':<25} {'Team':<15} → {'Cricsheet Name':<25} Score")
    print(f"  {'-'*80}")
    for r in results[:20]:
        cs = r['cricsheet'] or '❌ NO MATCH'
        print(f"  {r['squad']:<25} {r['team']:<15} → {cs:<25}  {r['score']}")

    if low_conf:
        print(f"\n⚠️  Low-confidence matches (score < 85) — review manually:")
        print(f"  {'Squad Name':<25} {'Team':<15} → {'Cricsheet':<25} Score")
        print(f"  {'-'*80}")
        for sq, tm, cs, sc in sorted(low_conf, key=lambda x: x[3]):
            print(f"  {sq:<25} {tm:<15} → {cs:<25}  {sc}")

    with engine.connect() as conn:
        mapped = conn.execute(text(
            "SELECT COUNT(*) FROM player_name_map WHERE cricsheet_player_name IS NOT NULL"
        )).scalar()
    print(f"\n✅ {mapped}/{total} squad players mapped to a Cricsheet player.")


if __name__ == "__main__":
    run_matching()
