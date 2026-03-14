"""
build_playing11_features.py
===========================
Generates a player-level dataset for Playing 11 Prediction.

Each row = one player from the squad, in the context of a specific match (team, venue, opponent).
The model can learn which 11 players are most valuable given:
  - Their historical batting/bowling stats
  - Their last 5 match form
  - How often they were actually selected (selection_rate)
  - The opponent and venue

Output: playing11_dataset.csv
"""
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://cricket_user:cricket_pass@localhost:5433/cricket_db")
engine = create_engine(DB_URL)

print("📥 Loading data from PostgreSQL...")

# ─────────────────────────────────────────────────────────────────────────────
# 1. Load squad players (name, team, role)
# ─────────────────────────────────────────────────────────────────────────────
squad_query = """
SELECT player_name, team, role, designation
FROM dim_squad_2026
ORDER BY team, player_name;
"""
df_squad = pd.read_sql(squad_query, engine)

# ─────────────────────────────────────────────────────────────────────────────
# 2. Load player name map to bridge to cricsheet names
# ─────────────────────────────────────────────────────────────────────────────
map_query = """
SELECT squad_player_name, squad_team, cricsheet_player_name
FROM player_name_map
WHERE cricsheet_player_name IS NOT NULL;
"""
df_map = pd.read_sql(map_query, engine)
# Build lookup: (squad_name, team) -> cricsheet_name
map_lookup = {
    (r['squad_player_name'], r['squad_team']): r['cricsheet_player_name']
    for _, r in df_map.iterrows()
}

# ─────────────────────────────────────────────────────────────────────────────
# 3. Load all matches involving squad teams (with venue + teams normalized)
# ─────────────────────────────────────────────────────────────────────────────
match_query = """
SELECT
    m.match_id,
    m.match_date,
    TRIM(SPLIT_PART(m.venue, ',', 1)) AS venue,
    CASE WHEN m.team1 = 'United States of America' THEN 'USA'
         WHEN m.team1 = 'United Arab Emirates' THEN 'UAE' ELSE m.team1 END AS team1,
    CASE WHEN m.team2 = 'United States of America' THEN 'USA'
         WHEN m.team2 = 'United Arab Emirates' THEN 'UAE' ELSE m.team2 END AS team2,
    CASE WHEN m.toss_winner = 'United States of America' THEN 'USA'
         WHEN m.toss_winner = 'United Arab Emirates' THEN 'UAE' ELSE m.toss_winner END AS toss_winner,
    m.toss_decision,
    CASE WHEN m.winner = 'United States of America' THEN 'USA'
         WHEN m.winner = 'United Arab Emirates' THEN 'UAE' ELSE m.winner END AS winner
FROM dim_match m
ORDER BY m.match_date ASC;
"""
df_matches = pd.read_sql(match_query, engine)
df_matches['match_date'] = pd.to_datetime(df_matches['match_date'])
squad_teams = df_squad['team'].unique().tolist()

# Squad-vs-Squad matches only for output
df_squad_matches = df_matches[
    df_matches['team1'].isin(squad_teams) & df_matches['team2'].isin(squad_teams)
].copy()

# ─────────────────────────────────────────────────────────────────────────────
# 4. Load per-player per-match batting stats
# ─────────────────────────────────────────────────────────────────────────────
print("🏏 Loading batting stats...")
bat_query = """
SELECT
    fd.match_id,
    m.match_date::date AS match_date,
    fd.batter AS cricsheet_name,
    SUM(fd.runs_batter) AS runs_scored,
    COUNT(*) AS balls_faced,
    COUNT(fd.wicket_type) FILTER (WHERE fd.player_out = fd.batter) AS dismissed
FROM fact_delivery fd
JOIN dim_match m ON fd.match_id = m.match_id
WHERE fd.inning IN (1, 2)
GROUP BY fd.match_id, m.match_date, fd.batter;
"""
df_bat = pd.read_sql(bat_query, engine)
df_bat['match_date'] = pd.to_datetime(df_bat['match_date'])

# ─────────────────────────────────────────────────────────────────────────────
# 5. Load per-player per-match bowling stats
# ─────────────────────────────────────────────────────────────────────────────
print("🎯 Loading bowling stats...")
bowl_query = """
SELECT
    fd.match_id,
    m.match_date::date AS match_date,
    fd.bowler AS cricsheet_name,
    SUM(fd.runs_total) AS runs_given,
    SUM(CASE WHEN fd.extra_wides = 0 AND fd.extra_noballs = 0 THEN 1 ELSE 0 END) AS legal_balls,
    COUNT(fd.wicket_type) AS wickets
FROM fact_delivery fd
JOIN dim_match m ON fd.match_id = m.match_id
WHERE fd.inning IN (1, 2)
GROUP BY fd.match_id, m.match_date, fd.bowler;
"""
df_bowl = pd.read_sql(bowl_query, engine)
df_bowl['match_date'] = pd.to_datetime(df_bowl['match_date'])

# ─────────────────────────────────────────────────────────────────────────────
# 6. Determine which players appeared in each match (for selection_rate)
# ─────────────────────────────────────────────────────────────────────────────
print("📋 Computing selection rates...")
# A player appeared in a match if they batted OR bowled
all_batters = df_bat[['match_id', 'cricsheet_name']].copy()
all_bowlers = df_bowl[['match_id', 'cricsheet_name']].copy()
df_appearances = pd.concat([all_batters, all_bowlers]).drop_duplicates()

# For each player, calculate overall selection_rate = matches appeared / total squad_team matches
def get_selection_rate(cricsheet_name, team, df_match, all_app):
    team_matches = df_match[(df_match['team1'] == team) | (df_match['team2'] == team)]
    total_team_matches = len(team_matches['match_id'].unique())
    if total_team_matches == 0:
        return 0.0
    player_appearances = all_app[all_app['cricsheet_name'] == cricsheet_name]['match_id'].nunique()
    return round(player_appearances / total_team_matches, 3)

# ─────────────────────────────────────────────────────────────────────────────
# 7. Compute per-player historical stats
# ─────────────────────────────────────────────────────────────────────────────
print("⚙️  Computing player career and form features...")

rows = []
total_players = len(df_squad)

for idx, player_row in df_squad.iterrows():
    player_name = player_row['player_name']
    team = player_row['team']
    role = player_row['role']
    designation = player_row['designation']

    # Get cricsheet name
    cs_name = map_lookup.get((player_name, team))
    if cs_name is None:
        # Try to find by just name (fallback if team wasn't stored)
        matches = df_map[df_map['squad_player_name'] == player_name]
        cs_name = matches.iloc[0]['cricsheet_player_name'] if len(matches) > 0 else None

    if cs_name is None:
        # Player not matched, fill with defaults
        rows.append({
            'team': team,
            'player_name': player_name,
            'player_role': role,
            'designation': designation,
            'batting_average': None,
            'strike_rate': None,
            'bowling_economy': None,
            'wickets_last_5_matches': 0,
            'runs_last_5_matches': 0,
            'selection_rate': 0.0,
            'career_total_runs': 0,
            'career_wickets': 0,
        })
        continue

    # ── Career batting stats ──
    p_bat = df_bat[df_bat['cricsheet_name'] == cs_name]
    career_runs = p_bat['runs_scored'].sum()
    career_balls = p_bat['balls_faced'].sum()
    career_outs = p_bat['dismissed'].sum()

    batting_avg = round(career_runs / max(career_outs, 1), 2)
    strike_rate = round(career_runs / max(career_balls, 1) * 100, 2)

    # ── Career bowling stats ──
    p_bowl = df_bowl[df_bowl['cricsheet_name'] == cs_name]
    career_legal_balls = p_bowl['legal_balls'].sum()
    career_runs_given = p_bowl['runs_given'].sum()
    career_wickets = p_bowl['wickets'].sum()

    bowling_economy = round(career_runs_given / max(career_legal_balls, 1) * 6, 2) if career_legal_balls > 0 else None

    # ── Last 5 matches form ──
    last5_matches = df_appearances[df_appearances['cricsheet_name'] == cs_name]['match_id'].unique()
    # Sort by date and take last 5
    m5 = df_matches[df_matches['match_id'].isin(last5_matches)].nlargest(5, 'match_date')['match_id'].tolist()

    runs_last_5 = p_bat[p_bat['match_id'].isin(m5)]['runs_scored'].sum()
    wickets_last_5 = p_bowl[p_bowl['match_id'].isin(m5)]['wickets'].sum()

    # ── Selection rate ──
    sel_rate = get_selection_rate(cs_name, team, df_matches, df_appearances)

    rows.append({
        'team': team,
        'player_name': player_name,
        'player_role': role,
        'designation': designation,
        'batting_average': batting_avg if career_balls > 0 else None,
        'strike_rate': strike_rate if career_balls > 0 else None,
        'bowling_economy': bowling_economy,
        'wickets_last_5_matches': int(wickets_last_5),
        'runs_last_5_matches': int(runs_last_5),
        'selection_rate': sel_rate,
        'career_total_runs': int(career_runs),
        'career_wickets': int(career_wickets),
    })

df_players = pd.DataFrame(rows)

# ─────────────────────────────────────────────────────────────────────────────
# 8. Cross the player table with opponent × venue contextual info
#    We take the DISTINCT opponent+venue pairs that each team has faced.
# ─────────────────────────────────────────────────────────────────────────────
print("🔗 Building per-match player rows with played target...")

# Build reverse lookup: cricsheet_name -> (squad_player_name, squad_team)
rev_map = {}
for (sq_name, sq_team), cs_name in map_lookup.items():
    if cs_name not in rev_map:
        rev_map[cs_name] = (sq_name, sq_team)

# Build a lookup from df_players indexed by (player_name, team)
df_players_idx = df_players.set_index(['player_name', 'team'])

context_rows = []
total_matches = len(df_squad_matches)

for i, (_, match) in enumerate(df_squad_matches.iterrows()):
    m_id = match['match_id']
    m_date = match['match_date']
    venue = match['venue']
    team1, team2 = match['team1'], match['team2']

    # Which cricsheet players appeared in this match?
    appeared = set(df_appearances[df_appearances['match_id'] == m_id]['cricsheet_name'].tolist())

    # For BOTH teams emit one row per squad player
    for team, opponent in [(team1, team2), (team2, team1)]:
        team_players = df_squad[df_squad['team'] == team]
        for _, player_row in team_players.iterrows():
            player_name = player_row['player_name']
            role = player_row['role']
            designation = player_row['designation']

            cs_name = map_lookup.get((player_name, team))

            # Was this player selected in this match?
            played = 1 if (cs_name and cs_name in appeared) else 0

            # Fetch precomputed career stats
            try:
                ps = df_players_idx.loc[(player_name, team)]
                batting_avg = ps['batting_average']
                sr = ps['strike_rate']
                bowl_econ = ps['bowling_economy']
                r5 = ps['runs_last_5_matches']
                w5 = ps['wickets_last_5_matches']
                sel_rate = ps['selection_rate']
                career_runs = ps['career_total_runs']
                career_wkts = ps['career_wickets']
            except KeyError:
                batting_avg = sr = bowl_econ = r5 = w5 = sel_rate = career_runs = career_wkts = 0

            # Venue-specific stats
            if cs_name:
                venue_match_ids = df_squad_matches[df_squad_matches['venue'] == venue]['match_id'].tolist()
                v_bat = df_bat[(df_bat['cricsheet_name'] == cs_name) & (df_bat['match_id'].isin(venue_match_ids))]
                venue_runs = int(v_bat['runs_scored'].sum())
                v_bowl = df_bowl[(df_bowl['cricsheet_name'] == cs_name) & (df_bowl['match_id'].isin(venue_match_ids))]
                venue_wickets = int(v_bowl['wickets'].sum())

                # ── Opponent-specific stats ──
                # Matches where this player's team faced this specific opponent (historically)
                opp_match_ids = df_squad_matches[
                    ((df_squad_matches['team1'] == team) & (df_squad_matches['team2'] == opponent)) |
                    ((df_squad_matches['team1'] == opponent) & (df_squad_matches['team2'] == team))
                ]['match_id'].tolist()

                # Batting vs opponent
                o_bat = df_bat[(df_bat['cricsheet_name'] == cs_name) & (df_bat['match_id'].isin(opp_match_ids))]
                runs_vs_opp = int(o_bat['runs_scored'].sum())
                outs_vs_opp = int(o_bat['dismissed'].sum())
                batting_avg_vs_opp = round(runs_vs_opp / max(outs_vs_opp, 1), 2) if len(o_bat) > 0 else 0.0

                # Bowling vs opponent
                o_bowl = df_bowl[(df_bowl['cricsheet_name'] == cs_name) & (df_bowl['match_id'].isin(opp_match_ids))]
                wickets_vs_opp = int(o_bowl['wickets'].sum())
                opp_legal_balls = o_bowl['legal_balls'].sum()
                opp_runs_given = o_bowl['runs_given'].sum()
                bowling_econ_vs_opp = round(opp_runs_given / max(opp_legal_balls, 1) * 6, 2) if opp_legal_balls > 0 else 0.0

                # Last 5 matches vs opponent form
                last5_vs_opp_ids = df_squad_matches[
                    ((df_squad_matches['team1'] == team) & (df_squad_matches['team2'] == opponent)) |
                    ((df_squad_matches['team1'] == opponent) & (df_squad_matches['team2'] == team))
                ].nlargest(5, 'match_date')['match_id'].tolist()
                wickets_last5_vs_opp = int(df_bowl[
                    (df_bowl['cricsheet_name'] == cs_name) & (df_bowl['match_id'].isin(last5_vs_opp_ids))
                ]['wickets'].sum())
            else:
                venue_runs = venue_wickets = 0
                runs_vs_opp = batting_avg_vs_opp = wickets_vs_opp = bowling_econ_vs_opp = wickets_last5_vs_opp = 0

            context_rows.append({
                'match_id': m_id,
                'match_date': m_date.date(),
                'team': team,
                'opponent': opponent,
                'venue': venue,
                'player_name': player_name,
                'player_role': role,
                'designation': designation,
                'batting_average': batting_avg,
                'strike_rate': sr,
                'bowling_economy': bowl_econ,
                'career_total_runs': career_runs,
                'career_wickets': career_wkts,
                'runs_last_5_matches': r5,
                'wickets_last_5_matches': w5,
                'runs_at_venue': venue_runs,
                'wickets_at_venue': venue_wickets,
                'runs_vs_opponent': runs_vs_opp,                     # ← NEW
                'batting_avg_vs_opponent': batting_avg_vs_opp,        # ← NEW
                'wickets_vs_opponent': wickets_vs_opp,                # ← NEW
                'bowling_econ_vs_opponent': bowling_econ_vs_opp,      # ← NEW
                'wickets_last5_vs_opponent': wickets_last5_vs_opp,    # ← NEW
                'selection_rate': sel_rate,
                'played': played,
            })

df_out = pd.DataFrame(context_rows)

# Fill missing numerics with 0
num_cols = ['batting_average', 'strike_rate', 'bowling_economy',
            'career_total_runs', 'career_wickets', 'runs_last_5_matches',
            'wickets_last_5_matches', 'runs_at_venue', 'wickets_at_venue', 'selection_rate']
df_out[num_cols] = df_out[num_cols].fillna(0)

OUTPUT_FILE = "playing11_dataset.csv"
df_out.to_csv(OUTPUT_FILE, index=False)

print(f"\n✅ Saved '{OUTPUT_FILE}' — {df_out.shape[0]} rows × {df_out.shape[1]} features")
played_pct = round(df_out['played'].mean() * 100, 1)
print(f"Played=1 represents {played_pct}% of rows (should be ~11/{len(df_squad['team'].unique())} players per match)")
print("\nColumns:", list(df_out.columns))



