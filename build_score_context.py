"""
build_score_context.py
======================
Generates an aggregated score-context dataset for score prediction.

Each row = unique (batting_team, bowling_team, venue) combination.

Features:
  venue_avg_*       — average scores AT this venue (all historical innings)
  team_avg_*        — batting_team's historical scoring averages (all innings)
  opponent_avg_*    — bowling_team's historical runs-conceded averages (all innings)

Output: score_context_dataset.csv
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://cricket_user:cricket_pass@localhost:5433/cricket_db")
engine = create_engine(DB_URL)

print("📥 Loading data from PostgreSQL...")

# ─────────────────────────────────────────────────────────────────────────────
# 1. Load all matches
# ─────────────────────────────────────────────────────────────────────────────
match_query = """
SELECT
    m.match_id,
    TRIM(SPLIT_PART(m.venue, ',', 1)) AS venue,
    CASE WHEN m.team1 = 'United States of America' THEN 'USA'
         WHEN m.team1 = 'United Arab Emirates'     THEN 'UAE' ELSE m.team1 END AS team1,
    CASE WHEN m.team2 = 'United States of America' THEN 'USA'
         WHEN m.team2 = 'United Arab Emirates'     THEN 'UAE' ELSE m.team2 END AS team2,
    CASE WHEN m.toss_winner = 'United States of America' THEN 'USA'
         WHEN m.toss_winner = 'United Arab Emirates'     THEN 'UAE' ELSE m.toss_winner END AS toss_winner,
    m.toss_decision,
    CASE WHEN m.winner = 'United States of America' THEN 'USA'
         WHEN m.winner = 'United Arab Emirates'     THEN 'UAE' ELSE m.winner END AS winner
FROM dim_match m;
"""
df_matches = pd.read_sql(match_query, engine)

# ─────────────────────────────────────────────────────────────────────────────
# 2. Load phase-wise stats from fact_delivery
# ─────────────────────────────────────────────────────────────────────────────
print("🏏 Loading phase delivery stats...")

phase_query = """
SELECT
    fd.match_id,
    fd.inning,
    SUM(CASE WHEN fd.over_number BETWEEN 1 AND 6   THEN fd.runs_total ELSE 0 END) AS pp_runs,
    SUM(CASE WHEN fd.over_number BETWEEN 7 AND 15  THEN fd.runs_total ELSE 0 END) AS mid_runs,
    SUM(CASE WHEN fd.over_number BETWEEN 16 AND 20 THEN fd.runs_total ELSE 0 END) AS death_runs,
    SUM(fd.runs_total) AS total_runs
FROM fact_delivery fd
WHERE fd.inning IN (1, 2)
GROUP BY fd.match_id, fd.inning;
"""
df_phase = pd.read_sql(phase_query, engine)

# ─────────────────────────────────────────────────────────────────────────────
# 3. Assign batting/bowling team per inning
# ─────────────────────────────────────────────────────────────────────────────
def batting_first_team(row):
    if row['toss_decision'] == 'bat':
        return row['toss_winner']
    return row['team2'] if row['toss_winner'] == row['team1'] else row['team1']

df_matches['bat_first'] = df_matches.apply(batting_first_team, axis=1)

df = df_phase.merge(df_matches[['match_id', 'venue', 'team1', 'team2', 'bat_first', 'winner']], on='match_id', how='inner')

def get_teams(row):
    if row['inning'] == 1:
        batting = row['bat_first']
        bowling = row['team2'] if row['bat_first'] == row['team1'] else row['team1']
    else:
        batting = row['team2'] if row['bat_first'] == row['team1'] else row['team1']
        bowling = row['bat_first']
    return pd.Series({'batting_team': batting, 'bowling_team': bowling})

df[['batting_team', 'bowling_team']] = df.apply(get_teams, axis=1)

# ─────────────────────────────────────────────────────────────────────────────
# 4. Load squad teams — we only want output rows for these teams
# ─────────────────────────────────────────────────────────────────────────────
squad_teams = pd.read_sql("SELECT DISTINCT team FROM dim_squad_2026", engine)['team'].tolist()

# ─────────────────────────────────────────────────────────────────────────────
# 5. Compute the three groups of averages
# ─────────────────────────────────────────────────────────────────────────────
print("📊 Computing venue, team, and opponent averages...")

# --- (A) Venue averages: avg runs scored at each venue (by any team) ---
venue_avg = (
    df.groupby('venue')[['pp_runs', 'mid_runs', 'death_runs', 'total_runs']]
    .mean()
    .round(2)
    .reset_index()
    .rename(columns={
        'pp_runs'    : 'venue_avg_pp_runs',
        'mid_runs'   : 'venue_avg_mid_runs',
        'death_runs' : 'venue_avg_death_runs',
        'total_runs' : 'venue_avg_runs',
    })
)

# --- (B) Batting team averages: how much each team scores overall ---
team_avg = (
    df.groupby('batting_team')[['pp_runs', 'mid_runs', 'death_runs', 'total_runs']]
    .mean()
    .round(2)
    .reset_index()
    .rename(columns={
        'batting_team': 'batting_team',
        'pp_runs'     : 'team_avg_pp_runs',
        'mid_runs'    : 'team_avg_mid_runs',
        'death_runs'  : 'team_avg_death_runs',
        'total_runs'  : 'team_avg_runs',
    })
)

# --- (C) Opponent (bowling team) averages: how many runs they concede overall ---
opp_avg = (
    df.groupby('bowling_team')[['pp_runs', 'mid_runs', 'death_runs', 'total_runs']]
    .mean()
    .round(2)
    .reset_index()
    .rename(columns={
        'bowling_team': 'bowling_team',
        'pp_runs'     : 'opponent_avg_pp_runs_conceded',
        'mid_runs'    : 'opponent_avg_mid_runs_conceded',
        'death_runs'  : 'opponent_avg_death_runs_conceded',
        'total_runs'  : 'opponent_avg_runs_conceded',
    })
)

# ─────────────────────────────────────────────────────────────────────────────
# 6. Build output: one row per (batting_team, bowling_team, venue) matchup
#    using only squad teams in both sides
# ─────────────────────────────────────────────────────────────────────────────
print("🔗 Building (batting_team, bowling_team, venue) combinations...")

# Get all unique (batting_team, bowling_team, venue) combos from historical data
combos = (
    df[df['batting_team'].isin(squad_teams) & df['bowling_team'].isin(squad_teams)]
    [['batting_team', 'bowling_team', 'venue']]
    .drop_duplicates()
    .reset_index(drop=True)
)

print(f"   Unique (batting_team × bowling_team × venue) combos: {len(combos)}")

# Join all three groups of averages
df_out = (
    combos
    .merge(venue_avg,  on='venue',        how='left')
    .merge(team_avg,   on='batting_team', how='left')
    .merge(opp_avg,    on='bowling_team', how='left')
)

# Reorder columns cleanly
df_out = df_out[[
    'batting_team',
    'bowling_team',
    'venue',

    'venue_avg_runs',
    'venue_avg_pp_runs',
    'venue_avg_mid_runs',
    'venue_avg_death_runs',

    'team_avg_runs',
    'team_avg_pp_runs',
    'team_avg_mid_runs',
    'team_avg_death_runs',

    'opponent_avg_runs_conceded',
    'opponent_avg_pp_runs_conceded',
    'opponent_avg_mid_runs_conceded',
    'opponent_avg_death_runs_conceded',
]]

# ─────────────────────────────────────────────────────────────────────────────
# 7. Save
# ─────────────────────────────────────────────────────────────────────────────
OUTPUT_FILE = "score_context_dataset.csv"
df_out.to_csv(OUTPUT_FILE, index=False)

print(f"\n✅ Saved '{OUTPUT_FILE}' — {df_out.shape[0]} rows × {df_out.shape[1]} columns")
print(f"\nSample rows:")
print(df_out.head(5).to_string(index=False))
print(f"\nColumns: {list(df_out.columns)}")
print(f"\nNaN check:\n{df_out.isna().sum()}")
