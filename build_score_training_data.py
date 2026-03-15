"""
build_score_training_data.py
============================
ULTIMATE VERSION (v3)
Generates an advanced innings-level dataset for Score Prediction.

Features:
  - Identity: batting_team, bowling_team, venue, innings, toss info
  - Career/Overall: venue_avg (runs/wkts), team_avg (runs), opponent_avg (conceded)
  - Form (Last 5): team scoring form, opponent conceding form
  - H2H: team win rate against specific opponent
  - Derived Ratios: attack_vs_defense, venue_vs_team, recent_vs_average
  - Targets: total_runs, phase_runs (PP, Mid, Death), phase_wickets (PP, Mid, Death)

Output: score_prediction_training_v3.csv
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

# 1. Load matches
match_query = """
SELECT
    m.match_id,
    m.match_date::date AS date,
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
FROM dim_match m
ORDER BY m.match_date ASC;
"""
df_matches = pd.read_sql(match_query, engine)
df_matches['date'] = pd.to_datetime(df_matches['date'])

# 2. Load phase stats (Runs AND Wickets)
phase_query = """
SELECT
    fd.match_id,
    fd.inning,
    SUM(CASE WHEN fd.over_number BETWEEN 1 AND 6   THEN fd.runs_total ELSE 0 END) AS pp_runs,
    SUM(CASE WHEN fd.over_number BETWEEN 7 AND 15  THEN fd.runs_total ELSE 0 END) AS mid_runs,
    SUM(CASE WHEN fd.over_number BETWEEN 16 AND 20 THEN fd.runs_total ELSE 0 END) AS death_runs,
    COUNT(CASE WHEN fd.over_number BETWEEN 1 AND 6  AND fd.wicket_type IS NOT NULL 
                    AND fd.wicket_type NOT IN ('run out', '') THEN 1 END) AS pp_wickets,
    COUNT(CASE WHEN fd.over_number BETWEEN 7 AND 15 AND fd.wicket_type IS NOT NULL 
                    AND fd.wicket_type NOT IN ('run out', '') THEN 1 END) AS mid_wickets,
    COUNT(CASE WHEN fd.over_number BETWEEN 16 AND 20 AND fd.wicket_type IS NOT NULL 
                    AND fd.wicket_type NOT IN ('run out', '') THEN 1 END) AS death_wickets,
    SUM(fd.runs_total) AS total_runs
FROM fact_delivery fd
WHERE fd.inning IN (1, 2)
GROUP BY fd.match_id, fd.inning;
"""
df_phase = pd.read_sql(phase_query, engine)

# 3. Join and assign teams
def batting_first_team(row):
    if row['toss_decision'] == 'bat': return row['toss_winner']
    return row['team2'] if row['toss_winner'] == row['team1'] else row['team1']

df_matches['bat_first'] = df_matches.apply(batting_first_team, axis=1)
df_all = df_phase.merge(df_matches, on='match_id', how='inner')

def get_teams(row):
    if row['inning'] == 1:
        batting, bowling = row['bat_first'], (row['team2'] if row['bat_first'] == row['team1'] else row['team1'])
    else:
        batting, bowling = (row['team2'] if row['bat_first'] == row['team1'] else row['team1']), row['bat_first']
    return pd.Series({'batting_team': batting, 'bowling_team': bowling})

df_all[['batting_team', 'bowling_team']] = df_all.apply(get_teams, axis=1)
df_all = df_all.sort_values('date').reset_index(drop=True)

# 4. Squad Filter
squad_teams = pd.read_sql("SELECT DISTINCT team FROM dim_squad_2026", engine)['team'].tolist()
df_squad_only = df_all[df_all['batting_team'].isin(squad_teams) & df_all['bowling_team'].isin(squad_teams)].copy()

# 5. Compute Advanced Features
print("⚙️  Computing ULTIMATE features (Venue Wkts, Derived Ratios, H2H)...")
output_rows = []
total = len(df_squad_only)

for i, row in df_squad_only.iterrows():
    if i % 300 == 0: print(f"  Row {i}/{total}...")

    m_date, venue, bat_t, bowl_t = row['date'], row['venue'], row['batting_team'], row['bowling_team']
    hist = df_all[df_all['date'] < m_date]

    # (A) Venue Stats (Runs & Wickets)
    v_hist = hist[hist['venue'] == venue]
    v_avgs = {
        'v_runs': round(v_hist['total_runs'].mean(), 2) if len(v_hist) > 0 else 160.0,
        'v_pp':   round(v_hist['pp_runs'].mean(), 2) if len(v_hist) > 0 else 45.0,
        'v_mid':  round(v_hist['mid_runs'].mean(), 2) if len(v_hist) > 0 else 70.0,
        'v_death':round(v_hist['death_runs'].mean(), 2) if len(v_hist) > 0 else 45.0,
        'v_pp_w': round(v_hist['pp_wickets'].mean(), 2) if len(v_hist) > 0 else 1.2,
        'v_mid_w':round(v_hist['mid_wickets'].mean(), 2) if len(v_hist) > 0 else 2.5,
        'v_death_w':round(v_hist['death_wickets'].mean(), 2) if len(v_hist) > 0 else 2.8,
    }

    # (B) Team Career Batting
    t_hist = hist[hist['batting_team'] == bat_t]
    t_avgs = {
        't_runs': round(t_hist['total_runs'].mean(), 2) if len(t_hist) > 0 else 155.0,
        't_pp':   round(t_hist['pp_runs'].mean(), 2) if len(t_hist) > 0 else 44.0,
        't_mid':  round(t_hist['mid_runs'].mean(), 2) if len(t_hist) > 0 else 68.0,
        't_death':round(t_hist['death_runs'].mean(), 2) if len(t_hist) > 0 else 43.0,
    }

    # (C) Opponent Career Bowling (Conceded)
    o_hist = hist[hist['bowling_team'] == bowl_t]
    o_avgs = {
        'o_runs': round(o_hist['total_runs'].mean(), 2) if len(o_hist) > 0 else 158.0,
        'o_pp':   round(o_hist['pp_runs'].mean(), 2) if len(o_hist) > 0 else 46.0,
        'o_mid':  round(o_hist['mid_runs'].mean(), 2) if len(o_hist) > 0 else 72.0,
        'o_death':round(o_hist['death_runs'].mean(), 2) if len(o_hist) > 0 else 40.0,
    }

    # (D) Recent Form (Last 5)
    t_l5_avg = round(t_hist.tail(5)['total_runs'].mean(), 2) if len(t_hist) >= 5 else t_avgs['t_runs']
    o_l5_avg = round(o_hist.tail(5)['total_runs'].mean(), 2) if len(o_hist) >= 5 else o_avgs['o_runs']

    # (E) H2H Win Rate
    h2h = hist[((hist['team1']==bat_t)&(hist['team2']==bowl_t))|((hist['team1']==bowl_t)&(hist['team2']==bat_t))].drop_duplicates('match_id')
    h2h_wr = round((h2h['winner']==bat_t).sum()/len(h2h), 3) if len(h2h)>0 else 0.5

    # (F) Derived Features
    attack_defense = round(t_avgs['t_runs'] / max(o_avgs['o_runs'], 1), 3)
    venue_team     = round(t_avgs['t_runs'] / max(v_avgs['v_runs'], 1), 3)
    momentum       = round(t_l5_avg / max(t_avgs['t_runs'], 1), 3)

    output_rows.append({
        'batting_team': bat_t, 'bowling_team': bowl_t, 'venue': venue, 'innings': row['inning'],
        'toss_winner': row['toss_winner'], 'toss_decision': row['toss_decision'],
        
        # Venue Avgs
        'venue_avg_runs': v_avgs['v_runs'], 'venue_avg_pp_runs': v_avgs['v_pp'], 'venue_avg_mid_runs': v_avgs['v_mid'], 'venue_avg_death_runs': v_avgs['v_death'],
        'venue_avg_pp_wickets': v_avgs['v_pp_w'], 'venue_avg_mid_wickets': v_avgs['v_mid_w'], 'venue_avg_death_wickets': v_avgs['v_death_w'],
        
        # Team Avgs
        'team_avg_runs': t_avgs['t_runs'], 'team_avg_pp_runs': t_avgs['t_pp'], 'team_avg_mid_runs': t_avgs['t_mid'], 'team_avg_death_runs': t_avgs['t_death'],
        
        # Opponent Avgs
        'opponent_avg_runs_conceded': o_avgs['o_runs'], 'opponent_avg_pp_runs_conceded': o_avgs['o_pp'], 'opponent_avg_mid_runs_conceded': o_avgs['o_mid'], 'opponent_avg_death_runs_conceded': o_avgs['o_death'],
        
        # Form & H2H
        'team_last5_avg_runs': t_l5_avg, 'opponent_last5_avg_conceded': o_l5_avg, 'team_win_rate_vs_opponent': h2h_wr,
        
        # Derived
        'attack_vs_defense': attack_defense, 'venue_vs_team': venue_team, 'recent_vs_average': momentum,
        
        # TARGETS
        'total_runs': int(row['total_runs']), 'pp_runs': int(row['pp_runs']), 'mid_runs': int(row['mid_runs']), 'death_runs': int(row['death_runs']),
        'pp_wickets': int(row['pp_wickets']), 'mid_wickets': int(row['mid_wickets']), 'death_wickets': int(row['death_wickets'])
    })

df_out = pd.DataFrame(output_rows)
OUT_FILE = "score_prediction_training_v3.csv"
df_out.to_csv(OUT_FILE, index=False)
print(f"✅ Saved '{OUT_FILE}' — {df_out.shape[0]} rows × {df_out.shape[1]} columns")
