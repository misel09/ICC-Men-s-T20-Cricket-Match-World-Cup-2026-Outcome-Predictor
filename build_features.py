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
# 1. Load ALL matches globally for rolling context (no squad filter here)
# ─────────────────────────────────────────────────────────────────────────────
all_match_query = """
SELECT
    match_id,
    match_date,
    TRIM(SPLIT_PART(venue, ',', 1)) AS venue,
    CASE WHEN team1 = 'United States of America' THEN 'USA'
         WHEN team1 = 'United Arab Emirates' THEN 'UAE' ELSE team1 END AS team1,
    CASE WHEN team2 = 'United States of America' THEN 'USA'
         WHEN team2 = 'United Arab Emirates' THEN 'UAE' ELSE team2 END AS team2,
    CASE WHEN toss_winner = 'United States of America' THEN 'USA'
         WHEN toss_winner = 'United Arab Emirates' THEN 'UAE' ELSE toss_winner END AS toss_winner,
    toss_decision,
    CASE WHEN winner = 'United States of America' THEN 'USA'
         WHEN winner = 'United Arab Emirates' THEN 'UAE' ELSE winner END AS winner
FROM dim_match
ORDER BY match_date ASC;
"""
df_all = pd.read_sql(all_match_query, engine)
df_all['match_date'] = pd.to_datetime(df_all['match_date'])

# ─────────────────────────────────────────────────────────────────────────────
# 2. Load innings totals for ALL matches
# ─────────────────────────────────────────────────────────────────────────────
inning_query = """
SELECT match_id, inning, SUM(runs_total) AS inning_runs
FROM fact_delivery WHERE inning IN (1,2) GROUP BY match_id, inning;
"""
df_inn = pd.read_sql(inning_query, engine)
i1 = df_inn[df_inn['inning'] == 1].rename(columns={'inning_runs': 'first_inning_runs'})[['match_id','first_inning_runs']]
i2 = df_inn[df_inn['inning'] == 2].rename(columns={'inning_runs': 'second_inning_runs'})[['match_id','second_inning_runs']]
df_all = df_all.merge(i1, on='match_id', how='left').merge(i2, on='match_id', how='left')

def get_batting_first(row):
    if row['toss_decision'] == 'bat':
        return row['toss_winner']
    other = row['team2'] if row['toss_winner'] == row['team1'] else row['team1']
    return other

df_all['first_innings_team'] = df_all.apply(get_batting_first, axis=1)

# ─────────────────────────────────────────────────────────────────────────────
# 3. Load per-player per-match delivery stats for rolling player features
# ─────────────────────────────────────────────────────────────────────────────
print("🏏 Loading player delivery stats...")
player_bat_query = """
SELECT fd.match_id, m.match_date::date AS match_date,
       fd.batter AS player_name,
       CASE WHEN m.team1 = 'United States of America' THEN 'USA'
            WHEN m.team1 = 'United Arab Emirates' THEN 'UAE' ELSE m.team1 END AS team1,
       CASE WHEN m.team2 = 'United States of America' THEN 'USA'
            WHEN m.team2 = 'United Arab Emirates' THEN 'UAE' ELSE m.team2 END AS team2,
       SUM(fd.runs_batter) AS runs_scored,
       COUNT(*) AS balls_faced
FROM fact_delivery fd
JOIN dim_match m ON fd.match_id = m.match_id
WHERE fd.inning IN (1, 2)
GROUP BY fd.match_id, m.match_date, fd.batter, m.team1, m.team2
"""
df_bat_match = pd.read_sql(player_bat_query, engine)
df_bat_match['match_date'] = pd.to_datetime(df_bat_match['match_date'])

player_bowl_query = """
SELECT fd.match_id, m.match_date::date AS match_date,
       fd.bowler AS player_name,
       CASE WHEN m.team1 = 'United States of America' THEN 'USA'
            WHEN m.team1 = 'United Arab Emirates' THEN 'UAE' ELSE m.team1 END AS team1,
       CASE WHEN m.team2 = 'United States of America' THEN 'USA'
            WHEN m.team2 = 'United Arab Emirates' THEN 'UAE' ELSE m.team2 END AS team2,
       SUM(fd.runs_total) AS runs_given,
       SUM(CASE WHEN fd.extra_wides = 0 AND fd.extra_noballs = 0 THEN 1 ELSE 0 END) AS legal_balls,
       COUNT(fd.wicket_type) AS wickets
FROM fact_delivery fd
JOIN dim_match m ON fd.match_id = m.match_id
WHERE fd.inning IN (1, 2)
GROUP BY fd.match_id, m.match_date, fd.bowler, m.team1, m.team2
"""
df_bowl_match = pd.read_sql(player_bowl_query, engine)
df_bowl_match['match_date'] = pd.to_datetime(df_bowl_match['match_date'])

# ─────────────────────────────────────────────────────────────────────────────
# 4. Load squad teams for output filter and player name map for team linking
# ─────────────────────────────────────────────────────────────────────────────
squad_teams = pd.read_sql("SELECT DISTINCT team FROM dim_squad_2026", engine)['team'].tolist()
player_map = pd.read_sql("SELECT cricsheet_player_name, squad_team FROM player_name_map WHERE squad_team IS NOT NULL", engine)
player_team_map = dict(zip(player_map['cricsheet_player_name'], player_map['squad_team']))

# ─────────────────────────────────────────────────────────────────────────────
# 5. Venue ↔ Country mapping for national-average imputation
# ─────────────────────────────────────────────────────────────────────────────
venue_country_map = {
    'Mumbai': 'India', 'Delhi': 'India', 'Kolkata': 'India', 'Chennai': 'India',
    'Bangalore': 'India', 'Ahmedabad': 'India', 'Hyderabad': 'India', 'Pune': 'India',
    'Lucknow': 'India', 'Dharamsala': 'India', 'Rajkot': 'India', 'Mohali': 'India',
    'Narendra Modi': 'India', 'Wankhede': 'India', 'Chinnaswamy': 'India',
    
    'Melbourne': 'Australia', 'Sydney': 'Australia', 'Brisbane': 'Australia',
    'Adelaide': 'Australia', 'Perth': 'Australia', 'Hobart': 'Australia',
    
    'London': 'England', 'Birmingham': 'England', 'Manchester': 'England',
    'Leeds': 'England', 'Nottingham': 'England', 'Southampton': 'England',
    'Cardiff': 'England', 'Bristol': 'England', 'The Oval': 'England',
    
    'Auckland': 'New Zealand', 'Wellington': 'New Zealand', 'Hamilton': 'New Zealand',
    'Christchurch': 'New Zealand', 'Napier': 'New Zealand', 'Dunedin': 'New Zealand',
    
    'Johannesburg': 'South Africa', 'Cape Town': 'South Africa', 'Centurion': 'South Africa',
    'Durban': 'South Africa', 'Port Elizabeth': 'South Africa', 'Bloemfontein': 'South Africa',
    
    'Lahore': 'Pakistan', 'Karachi': 'Pakistan', 'Rawalpindi': 'Pakistan', 'Multan': 'Pakistan',
    
    'Colombo': 'Sri Lanka', 'Kandy': 'Sri Lanka', 'Galle': 'Sri Lanka',
    'Hambantota': 'Sri Lanka', 'Dambulla': 'Sri Lanka',
    
    'Dubai': 'UAE', 'Abu Dhabi': 'UAE', 'Sharjah': 'UAE',
    
    'Lauderhill': 'USA', 'Grand Prairie': 'USA', 'New York': 'USA', 'Nassau': 'USA',
    
    'Providence': 'West Indies', 'Antigua': 'West Indies', 'Barbados': 'West Indies',
    'Kingston': 'West Indies', 'Trinidad': 'West Indies', 'St Lucia': 'West Indies',
    
    'Dhaka': 'Bangladesh', 'Chattogram': 'Bangladesh', 'Sylhet': 'Bangladesh',
    
    'Kabul': 'Afghanistan', 'Sharjah': 'UAE',
}

# Pre-compute national-level averages for fallback imputation
venue_scores = df_all.groupby('venue')['first_inning_runs'].mean().reset_index()
venue_scores.columns = ['venue', 'national_avg_score']

def get_venue_country(ven):
    for kw, country in venue_country_map.items():
        if kw in ven:
            return country
    return 'Unknown'

venue_scores['country'] = venue_scores['venue'].apply(get_venue_country)
national_avg = venue_scores.groupby('country')['national_avg_score'].mean()
global_avg = df_all['first_inning_runs'].mean()

# ─────────────────────────────────────────────────────────────────────────────
# 6. Home advantage helper
# ─────────────────────────────────────────────────────────────────────────────
home_venue_map = {
    'India': ['Mumbai', 'Delhi', 'Kolkata', 'Chennai', 'Bangalore', 'Ahmedabad',
              'Hyderabad', 'Pune', 'Lucknow', 'Rajkot', 'Mohali', 'Narendra Modi', 'Wankhede', 'Chinnaswamy'],
    'Australia': ['Melbourne', 'Sydney', 'Brisbane', 'Adelaide', 'Perth', 'Hobart'],
    'England': ['London', 'Birmingham', 'Manchester', 'Leeds', 'Nottingham',
                'Southampton', 'Cardiff', 'Bristol', 'The Oval'],
    'New Zealand': ['Auckland', 'Wellington', 'Hamilton', 'Christchurch', 'Napier', 'Dunedin'],
    'South Africa': ['Johannesburg', 'Cape Town', 'Centurion', 'Durban', 'Port Elizabeth', 'Bloemfontein'],
    'Pakistan': ['Lahore', 'Karachi', 'Rawalpindi', 'Multan'],
    'Sri Lanka': ['Colombo', 'Kandy', 'Galle', 'Hambantota', 'Dambulla'],
    'UAE': ['Dubai', 'Abu Dhabi', 'Sharjah'],
    'USA': ['Lauderhill', 'Grand Prairie', 'New York', 'Nassau'],
    'West Indies': ['Providence', 'Antigua', 'Barbados', 'Kingston', 'Trinidad', 'St Lucia'],
    'Bangladesh': ['Dhaka', 'Chattogram', 'Sylhet'],
    'Afghanistan': ['Kabul', 'Kandahar'],
    'Ireland': ['Dublin', 'Belfast'],
    'Scotland': ['Edinburgh', 'Glasgow'],
}

def get_home_adv(t1, t2, ven):
    for city in home_venue_map.get(t1, []):
        if city in ven: return 1
    for city in home_venue_map.get(t2, []):
        if city in ven: return -1
    return 0

# ─────────────────────────────────────────────────────────────────────────────
# 7. Compute all features per squad-vs-squad match
# ─────────────────────────────────────────────────────────────────────────────
# Filter output rows to squad-vs-squad only
df_squad_matches = df_all[
    df_all['team1'].isin(squad_teams) & df_all['team2'].isin(squad_teams) &
    df_all['winner'].notna()
].copy()

print(f"Total historical matches loaded: {len(df_all)}")
print(f"Squad-vs-squad matches (output rows): {len(df_squad_matches)}")
print("⚙️  Computing point-in-time rolling features...")

feature_rows = []
total = len(df_squad_matches)

for i, (_, row) in enumerate(df_squad_matches.iterrows()):
    if i % 100 == 0:
        print(f"  Processing match {i}/{total}...")

    m_id = row['match_id']
    m_date = row['match_date']
    t1, t2 = row['team1'], row['team2']
    v = row['venue']
    first_inn_team = row['first_innings_team']
    target = 1 if row['winner'] == t1 else 0
    is_t1_batting_first = 1 if first_inn_team == t1 else 0

    # Historical context = everything BEFORE this match date
    hist = df_all[df_all['match_date'] < m_date]

    # ── Team Win Rates ──
    def win_rate(team, df_hist, last_n=None):
        g = df_hist[(df_hist['team1'] == team) | (df_hist['team2'] == team)]
        if last_n: g = g.tail(last_n)
        if len(g) == 0: return None
        return (g['winner'] == team).sum() / len(g)

    t1_overall_wr = win_rate(t1, hist) or 0.5
    t2_overall_wr = win_rate(t2, hist) or 0.5
    recent_form_t1 = win_rate(t1, hist, 10) or t1_overall_wr
    recent_form_t2 = win_rate(t2, hist, 10) or t2_overall_wr

    # ── H2H ──
    h2h = hist[((hist['team1']==t1)&(hist['team2']==t2))|((hist['team1']==t2)&(hist['team2']==t1))]
    h2h_win_rate = (h2h['winner'] == t1).sum() / len(h2h) if len(h2h) > 0 else None

    # ── Venue Stats ──
    v_hist = hist[hist['venue'] == v]
    if len(v_hist) >= 5:
        avg_score_venue = v_hist['first_inning_runs'].mean()
        bat_first_wins = (v_hist['winner'] == v_hist['first_innings_team']).sum()
        toss_venue_win_rate = bat_first_wins / len(v_hist)
    else:
        # Fallback to national average
        country = get_venue_country(v)
        avg_score_venue = national_avg.get(country, global_avg)
        toss_venue_win_rate = hist['winner'].eq(hist['first_innings_team']).mean() if len(hist) > 0 else 0.52

    # ── Venue Win Rates per team ──
    def venue_wr(team, df_v):
        g = df_v[(df_v['team1'] == team) | (df_v['team2'] == team)]
        if len(g) < 3: return None
        return (g['winner'] == team).sum() / len(g)

    venue_win_rate_t1 = venue_wr(t1, v_hist)
    venue_win_rate_t2 = venue_wr(t2, v_hist)

    # ── Player Features: Rolling Batting Avg (top 3 batsmen, last 10 matches) ──
    def team_batting_strength(team, date, bat_df, n_matches=10, top_n=3):
        # Get all players who have played for this team before cutoff
        # Use player_name_map to link names to squad team
        team_players = [p for p, t in player_team_map.items() if t == team]
        if not team_players:
            return None
        
        player_recent_avgs = []
        for player in team_players:
            p_data = bat_df[(bat_df['player_name'] == player) &
                            (bat_df['match_date'] < date)]
            if len(p_data) == 0:
                continue
            last_n = p_data.tail(n_matches)
            if last_n['balls_faced'].sum() == 0:
                continue
            sr = last_n['runs_scored'].sum() / last_n['balls_faced'].sum() * 100
            avg = last_n['runs_scored'].mean()
            player_recent_avgs.append(avg)
        
        if not player_recent_avgs:
            return None
        top = sorted(player_recent_avgs, reverse=True)[:top_n]
        return np.mean(top)

    def team_bowling_strength(team, date, bowl_df, n_matches=10, top_n=3):
        team_players = [p for p, t in player_team_map.items() if t == team]
        if not team_players:
            return None
        
        player_econs = []
        for player in team_players:
            p_data = bowl_df[(bowl_df['player_name'] == player) &
                             (bowl_df['match_date'] < date)]
            if len(p_data) == 0:
                continue
            last_n = p_data.tail(n_matches)
            total_balls = last_n['legal_balls'].sum()
            if total_balls == 0:
                continue
            econ = last_n['runs_given'].sum() / total_balls * 6
            player_econs.append(econ)
        
        if not player_econs:
            return None
        # Best bowlers = lowest economy
        top = sorted(player_econs)[:top_n]
        return np.mean(top)

    t1_bat_str = team_batting_strength(t1, m_date, df_bat_match)
    t2_bat_str = team_batting_strength(t2, m_date, df_bat_match)
    t1_bowl_econ = team_bowling_strength(t1, m_date, df_bowl_match)
    t2_bowl_econ = team_bowling_strength(t2, m_date, df_bowl_match)

    feature_rows.append({
        'match_id': m_id,
        'team1': t1,
        'team2': t2,
        'venue': v,
        'first_innings_team': first_inn_team,
        'is_team1_batting_first': is_t1_batting_first,
        'home_advantage': get_home_adv(t1, t2, v),

        # Target-correlated features
        'h2h_win_rate': round(h2h_win_rate, 3) if h2h_win_rate is not None else None,
        'recent_form_t1': round(recent_form_t1, 3),
        'recent_form_t2': round(recent_form_t2, 3),
        'team1_overall_win_rate': round(t1_overall_wr, 3),
        'team2_overall_win_rate': round(t2_overall_wr, 3),

        # Venue features
        'avg_score_venue': round(avg_score_venue, 1),
        'toss_venue_win_rate': round(toss_venue_win_rate, 3),
        'venue_win_rate_t1': round(venue_win_rate_t1, 3) if venue_win_rate_t1 is not None else None,
        'venue_win_rate_t2': round(venue_win_rate_t2, 3) if venue_win_rate_t2 is not None else None,

        # Player features
        't1_top_batsmen_avg': round(t1_bat_str, 2) if t1_bat_str is not None else None,
        't2_top_batsmen_avg': round(t2_bat_str, 2) if t2_bat_str is not None else None,
        't1_top_bowler_econ': round(t1_bowl_econ, 2) if t1_bowl_econ is not None else None,
        't2_top_bowler_econ': round(t2_bowl_econ, 2) if t2_bowl_econ is not None else None,

        'team1_won': target
    })

df_feat = pd.DataFrame(feature_rows)

# ─────────────────────────────────────────────────────────────────────────────
# 8. Imputation: fill None player features with team medians
# ─────────────────────────────────────────────────────────────────────────────
for col in ['h2h_win_rate', 'venue_win_rate_t1', 'venue_win_rate_t2',
            't1_top_batsmen_avg', 't2_top_batsmen_avg',
            't1_top_bowler_econ', 't2_top_bowler_econ']:
    df_feat[col] = df_feat[col].fillna(df_feat[col].median())

# ─────────────────────────────────────────────────────────────────────────────
# 9. Add pitch type and save
# ─────────────────────────────────────────────────────────────────────────────
def pitch_type(score):
    if score > 165: return 'batting_friendly'
    if score < 140: return 'bowling_friendly'
    return 'balanced'

df_feat['pitch_type'] = df_feat['avg_score_venue'].apply(pitch_type)

OUTPUT_FILE = "feature_dataset_v3.csv"
df_feat.to_csv(OUTPUT_FILE, index=False)

print(f"\n✅ Saved '{OUTPUT_FILE}' — Shape: {df_feat.shape[0]} rows × {df_feat.shape[1]} columns")
print(f"\nNon-placeholder stats (% of real values):")
for col in ['h2h_win_rate', 'venue_win_rate_t1', 'venue_win_rate_t2', 'avg_score_venue']:
    unique_vals = df_feat[col].nunique()
    print(f"  {col}: {unique_vals} unique values")
print("\nColumn list:", list(df_feat.columns))
