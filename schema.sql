-- schema.sql – Database definition for T20 World Cup predictive modeling

-- ────────────────────────────────────────────────────────────
-- CLEANUP
-- ────────────────────────────────────────────────────────────
DROP VIEW IF EXISTS vw_phase_analysis CASCADE;
DROP VIEW IF EXISTS vw_innings_score CASCADE;
DROP VIEW IF EXISTS vw_dot_ball_stats CASCADE;
DROP VIEW IF EXISTS vw_bowling_average CASCADE;
DROP VIEW IF EXISTS vw_batting_average CASCADE;
DROP VIEW IF EXISTS vw_venue_stats CASCADE;
DROP VIEW IF EXISTS vw_team_head_to_head CASCADE;
DROP VIEW IF EXISTS vw_bowler_stats CASCADE;
DROP VIEW IF EXISTS vw_batter_stats CASCADE;
DROP TABLE IF EXISTS fact_delivery CASCADE;
DROP TABLE IF EXISTS dim_match CASCADE;
DROP TABLE IF EXISTS dim_player CASCADE;
DROP TABLE IF EXISTS dim_team CASCADE;
DROP TABLE IF EXISTS dim_squad_2026 CASCADE;
DROP TABLE IF EXISTS player_name_map CASCADE;

-- ────────────────────────────────────────────────────────────
-- BRONZE LAYER
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_match (
    match_id        SERIAL PRIMARY KEY,
    match_file      TEXT UNIQUE,
    match_date      DATE NOT NULL,
    season          TEXT,
    venue           TEXT NOT NULL,
    city            TEXT,
    team1           TEXT NOT NULL,
    team2           TEXT NOT NULL,
    toss_winner     TEXT,
    toss_decision   TEXT,
    winner          TEXT,
    win_by_runs     INT,
    win_by_wickets  INT,
    player_of_match TEXT,
    balls_per_over  INT DEFAULT 6
);

CREATE TABLE IF NOT EXISTS dim_squad_2026 (
    squad_id        SERIAL PRIMARY KEY,
    player_name     TEXT NOT NULL,
    team            TEXT NOT NULL,
    role            TEXT,
    designation     TEXT,
    UNIQUE (player_name, team)
);

CREATE TABLE IF NOT EXISTS player_name_map (
    id                    SERIAL PRIMARY KEY,
    squad_player_name     TEXT NOT NULL,
    squad_team            TEXT NOT NULL,
    cricsheet_player_name TEXT,
    match_score           INT,
    verified              BOOLEAN DEFAULT FALSE,
    UNIQUE (squad_player_name, squad_team)
);

CREATE TABLE IF NOT EXISTS dim_player (
    player_id       TEXT PRIMARY KEY,
    player_name     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_team (
    team_id   SERIAL PRIMARY KEY,
    team_name TEXT NOT NULL UNIQUE
);

-- ────────────────────────────────────────────────────────────
-- SILVER LAYER
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_delivery (
    delivery_id     BIGSERIAL PRIMARY KEY,
    match_id        INT REFERENCES dim_match(match_id) ON DELETE CASCADE,
    inning          INT,
    over_number     INT,
    ball_number     INT,
    batter          TEXT,  
    bowler          TEXT, 
    non_striker     TEXT,
    runs_batter     INT DEFAULT 0,
    runs_extras     INT DEFAULT 0,
    runs_total      INT DEFAULT 0,
    extra_wides     INT DEFAULT 0,
    extra_noballs   INT DEFAULT 0,
    extra_byes      INT DEFAULT 0,
    extra_legbyes   INT DEFAULT 0,
    wicket_type     TEXT,
    player_out      TEXT
);

-- ────────────────────────────────────────────────────────────
-- GOLD LAYER (OVERALL STATS FOR SQUAD PLAYERS ONLY)
-- ────────────────────────────────────────────────────────────

-- 1. BATTER STATS (OVERALL)
CREATE OR REPLACE VIEW vw_batter_stats AS
SELECT
    sq.player_name,
    sq.team,
    SUM(d.runs_batter) AS total_runs,
    COUNT(*) AS balls_faced,
    SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
    SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END) AS sixes,
    ROUND(SUM(d.runs_batter)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS strike_rate
FROM fact_delivery d
JOIN player_name_map nm ON d.batter = nm.cricsheet_player_name
JOIN dim_squad_2026 sq  ON nm.squad_player_name = sq.player_name AND nm.squad_team = sq.team
GROUP BY sq.player_name, sq.team;

-- 2. BOWLER STATS (OVERALL)
CREATE OR REPLACE VIEW vw_bowler_stats AS
SELECT
    sq.player_name,
    sq.team,
    COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0) AS legal_balls,
    SUM(d.runs_total) AS runs_conceded,
    COUNT(d.wicket_type) AS wickets,
    ROUND(SUM(d.runs_total)::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0), 0) * 6, 2) AS economy_rate
FROM fact_delivery d
JOIN player_name_map nm ON d.bowler = nm.cricsheet_player_name
JOIN dim_squad_2026 sq  ON nm.squad_player_name = sq.player_name AND nm.squad_team = sq.team
GROUP BY sq.player_name, sq.team;

-- 3. BATTING AVERAGE (OVERALL)
CREATE OR REPLACE VIEW vw_batting_average AS
WITH match_runs AS (
    SELECT batter, match_id, SUM(runs_batter) AS match_total
    FROM fact_delivery
    GROUP BY batter, match_id
),
milestones AS (
    SELECT batter,
           SUM(CASE WHEN match_total >= 50 AND match_total < 100 THEN 1 ELSE 0 END) AS fifties,
           SUM(CASE WHEN match_total >= 100 THEN 1 ELSE 0 END) AS hundreds
    FROM match_runs
    GROUP BY batter
)
SELECT
    sq.player_name,
    sq.team,
    SUM(d.runs_batter) AS career_runs,
    COUNT(DISTINCT d.match_id) AS matches_played,
    COUNT(d.wicket_type) FILTER (WHERE d.player_out = d.batter) AS total_dismissals,
    ROUND(SUM(d.runs_batter)::NUMERIC / NULLIF(COUNT(d.wicket_type) FILTER (WHERE d.player_out = d.batter), 0), 2) AS batting_average,
    COALESCE(m.fifties, 0) AS fifties,
    COALESCE(m.hundreds, 0) AS hundreds
FROM fact_delivery d
JOIN player_name_map nm ON d.batter = nm.cricsheet_player_name
JOIN dim_squad_2026 sq  ON nm.squad_player_name = sq.player_name AND nm.squad_team = sq.team
LEFT JOIN milestones m ON m.batter = d.batter
GROUP BY sq.player_name, sq.team, m.fifties, m.hundreds;

-- 4. BOWLING AVERAGE (OVERALL)
CREATE OR REPLACE VIEW vw_bowling_average AS
SELECT
    sq.player_name,
    sq.team,
    COUNT(DISTINCT d.match_id) AS matches_played,
    COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0) AS legal_balls,
    SUM(d.runs_total) AS runs_conceded,
    COUNT(d.wicket_type) AS total_wickets,
    ROUND(SUM(d.runs_total)::NUMERIC / NULLIF(COUNT(d.wicket_type), 0), 2) AS bowling_average,
    ROUND(COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0)::NUMERIC / NULLIF(COUNT(d.wicket_type), 0), 2) AS bowling_strike_rate,
    ROUND(SUM(d.runs_total)::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0), 0) * 6, 2) AS economy_rate
FROM fact_delivery d
JOIN player_name_map nm ON d.bowler = nm.cricsheet_player_name
JOIN dim_squad_2026 sq  ON nm.squad_player_name = sq.player_name AND nm.squad_team = sq.team
GROUP BY sq.player_name, sq.team;

-- 5. DOT BALL STATS (OVERALL)
CREATE OR REPLACE VIEW vw_dot_ball_stats AS
SELECT
    sq.player_name,
    sq.team,
    COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0) AS total_legal_balls,
    COUNT(*) FILTER (WHERE d.runs_total = 0 AND d.extra_wides = 0 AND d.extra_noballs = 0) AS dot_balls,
    ROUND(COUNT(*) FILTER (WHERE d.runs_total = 0 AND d.extra_wides = 0 AND d.extra_noballs = 0)::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0), 0) * 100, 2) AS dot_ball_percentage
FROM fact_delivery d
JOIN player_name_map nm ON d.bowler = nm.cricsheet_player_name
JOIN dim_squad_2026 sq  ON nm.squad_player_name = sq.player_name AND nm.squad_team = sq.team
GROUP BY sq.player_name, sq.team;

-- 6. TEAM HEAD TO HEAD (SQUAD TEAMS ONLY)
CREATE OR REPLACE VIEW vw_team_head_to_head AS
WITH mapped_teams AS (
    SELECT 
        CASE 
            WHEN team1 = 'United States of America' THEN 'USA'
            WHEN team1 = 'United Arab Emirates' THEN 'UAE'
            ELSE team1 
        END AS m_team1,
        CASE 
            WHEN team2 = 'United States of America' THEN 'USA'
            WHEN team2 = 'United Arab Emirates' THEN 'UAE'
            ELSE team2 
        END AS m_team2,
        CASE 
            WHEN winner = 'United States of America' THEN 'USA'
            WHEN winner = 'United Arab Emirates' THEN 'UAE'
            ELSE winner 
        END AS m_winner
    FROM dim_match
)
SELECT
    LEAST(m_team1, m_team2) AS team_a,
    GREATEST(m_team1, m_team2) AS team_b,
    COUNT(*) AS total_matches,
    SUM(CASE WHEN m_winner = LEAST(m_team1, m_team2) THEN 1 ELSE 0 END) AS team_a_wins,
    SUM(CASE WHEN m_winner = GREATEST(m_team1, m_team2) THEN 1 ELSE 0 END) AS team_b_wins,
    SUM(CASE WHEN m_winner IS NULL THEN 1 ELSE 0 END) AS no_result
FROM mapped_teams
WHERE m_team1 IN (SELECT DISTINCT team FROM dim_squad_2026)
  AND m_team2 IN (SELECT DISTINCT team FROM dim_squad_2026)
GROUP BY LEAST(m_team1, m_team2), GREATEST(m_team1, m_team2);

-- 7. VENUE STATS (1ST & 2ND INNINGS AVG)
CREATE OR REPLACE VIEW vw_venue_stats AS
WITH normalized_matches AS (
    SELECT 
        match_id,
        -- Many stadiums are duplicated with ", CityName" appended.
        -- We split by the first comma and take the first part to normalize them automatically.
        TRIM(SPLIT_PART(venue, ',', 1)) AS clean_venue
    FROM dim_match
)
SELECT
    m.clean_venue AS venue,
    COUNT(DISTINCT m.match_id) AS total_matches,
    ROUND(AVG(i1.total), 2) AS avg_first_innings_score,
    ROUND(AVG(i2.total), 2) AS avg_second_innings_score
FROM normalized_matches m
LEFT JOIN (
    SELECT match_id, SUM(runs_total) AS total FROM fact_delivery WHERE inning = 1 GROUP BY match_id
) i1 ON m.match_id = i1.match_id
LEFT JOIN (
    SELECT match_id, SUM(runs_total) AS total FROM fact_delivery WHERE inning = 2 GROUP BY match_id
) i2 ON m.match_id = i2.match_id
GROUP BY m.clean_venue;

-- 8. INNINGS SCORE (SQUAD TEAMS ONLY)
CREATE OR REPLACE VIEW vw_innings_score AS
SELECT
    d.match_id,
    m.match_date,
    m.venue,
    m.team1,
    m.team2,
    m.winner,
    m.toss_winner,
    m.toss_decision,
    d.inning,
    CASE WHEN d.inning = 1 THEN m.team1 ELSE m.team2 END AS batting_team,
    CASE WHEN d.inning = 1 THEN m.team2 ELSE m.team1 END AS bowling_team,
    SUM(d.runs_total) AS total_runs,
    COUNT(d.wicket_type) AS total_wickets,
    COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0) AS total_balls
FROM fact_delivery d
JOIN dim_match m ON d.match_id = m.match_id
WHERE m.team1 IN (SELECT DISTINCT team FROM dim_squad_2026)
  AND m.team2 IN (SELECT DISTINCT team FROM dim_squad_2026)
GROUP BY d.match_id, m.match_date, m.venue, m.team1, m.team2, m.winner, m.toss_winner, m.toss_decision, d.inning;

-- 9. PHASE ANALYSIS (SQUAD TEAMS ONLY)
CREATE OR REPLACE VIEW vw_phase_analysis AS
SELECT
    d.match_id,
    m.match_date,
    m.venue,
    d.inning,
    CASE
        WHEN d.over_number BETWEEN 0 AND 5 THEN 'Powerplay (0-5)'
        WHEN d.over_number BETWEEN 6 AND 14 THEN 'Middle Overs (6-14)'
        ELSE 'Death Overs (15-19)'
    END AS match_phase,
    SUM(d.runs_total) AS runs_scored,
    COUNT(d.wicket_type) AS wickets_lost,
    ROUND(SUM(d.runs_total)::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0), 0) * 6, 2) AS run_rate
FROM fact_delivery d
JOIN dim_match m ON d.match_id = m.match_id
WHERE m.team1 IN (SELECT DISTINCT team FROM dim_squad_2026)
  AND m.team2 IN (SELECT DISTINCT team FROM dim_squad_2026)
GROUP BY d.match_id, m.match_date, m.venue, d.inning, match_phase;
