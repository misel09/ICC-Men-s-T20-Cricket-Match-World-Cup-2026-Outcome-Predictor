-- 1. KEEP ONLY SQUAD TEAMS IN DIM_MATCH 
-- This automatically cascades to fact_delivery and cleans non-relevant matches
DELETE FROM dim_match 
WHERE team1 NOT IN (SELECT DISTINCT team FROM dim_squad_2026)
   OR team2 NOT IN (SELECT DISTINCT team FROM dim_squad_2026);

-- 2. DROP FOREIGN KEYS ON FACT_DELIVERY SO WE CAN INSERT NAMES
ALTER TABLE fact_delivery DROP CONSTRAINT IF EXISTS fact_delivery_batter_fkey;
ALTER TABLE fact_delivery DROP CONSTRAINT IF EXISTS fact_delivery_bowler_fkey;
ALTER TABLE fact_delivery DROP CONSTRAINT IF EXISTS fact_delivery_non_striker_fkey;
ALTER TABLE fact_delivery DROP CONSTRAINT IF EXISTS fact_delivery_player_out_fkey;

-- 3. CREATE MAPPER FOR UUID -> FULL SQUAD NAME
CREATE TEMP TABLE uuid_map AS
SELECT p.player_id, 
       COALESCE(nm.squad_player_name, p.player_name) AS final_name
FROM dim_player p
LEFT JOIN player_name_map nm ON p.player_name = nm.cricsheet_player_name;

CREATE INDEX ON uuid_map(player_id);

-- 4. REPLACE UUIDs WITH FULL NAMES IN FACT_DELIVERY
UPDATE fact_delivery d
SET batter = (SELECT final_name FROM uuid_map WHERE player_id = d.batter),
    bowler = (SELECT final_name FROM uuid_map WHERE player_id = d.bowler),
    non_striker = (SELECT final_name FROM uuid_map WHERE player_id = d.non_striker),
    player_out = (SELECT final_name FROM uuid_map WHERE player_id = d.player_out)
WHERE d.batter IN (SELECT player_id FROM uuid_map)
   OR d.bowler IN (SELECT player_id FROM uuid_map)
   OR d.non_striker IN (SELECT player_id FROM uuid_map)
   OR d.player_out IN (SELECT player_id FROM uuid_map);

-- 5. DROP OLD CONFLICTING VIEWS
DROP VIEW IF EXISTS vw_phase_analysis CASCADE;
DROP VIEW IF EXISTS vw_innings_score CASCADE;
DROP VIEW IF EXISTS vw_dot_ball_stats CASCADE;
DROP VIEW IF EXISTS vw_bowling_average CASCADE;
DROP VIEW IF EXISTS vw_batting_average CASCADE;
DROP VIEW IF EXISTS vw_venue_stats CASCADE;
DROP VIEW IF EXISTS vw_team_head_to_head CASCADE;
DROP VIEW IF EXISTS vw_bowler_stats CASCADE;
DROP VIEW IF EXISTS vw_batter_stats CASCADE;

-- 6. RECREATE VIEWS OVERALL USING FACT_DELIVERY DIRECTLY (NOT DATE-WISE)
CREATE OR REPLACE VIEW vw_batter_stats AS
SELECT
    d.batter AS player_name,
    sq.team,
    SUM(d.runs_batter) AS total_runs,
    COUNT(*) AS balls_faced,
    SUM(CASE WHEN d.runs_batter = 4 THEN 1 ELSE 0 END) AS fours,
    SUM(CASE WHEN d.runs_batter = 6 THEN 1 ELSE 0 END) AS sixes,
    ROUND(SUM(d.runs_batter)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS strike_rate
FROM fact_delivery d
JOIN dim_squad_2026 sq ON d.batter = sq.player_name
GROUP BY d.batter, sq.team;

CREATE OR REPLACE VIEW vw_bowler_stats AS
SELECT
    d.bowler AS player_name,
    sq.team,
    COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0) AS legal_balls,
    SUM(d.runs_total) AS runs_conceded,
    COUNT(d.wicket_type) AS wickets,
    ROUND(SUM(d.runs_total)::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0), 0) * 6, 2) AS economy_rate
FROM fact_delivery d
JOIN dim_squad_2026 sq ON d.bowler = sq.player_name
GROUP BY d.bowler, sq.team;

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
JOIN dim_squad_2026 sq ON d.batter = sq.player_name
LEFT JOIN milestones m ON m.batter = d.batter
GROUP BY sq.player_name, sq.team, m.fifties, m.hundreds;

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
JOIN dim_squad_2026 sq ON d.bowler = sq.player_name
GROUP BY sq.player_name, sq.team;

CREATE OR REPLACE VIEW vw_dot_ball_stats AS
SELECT
    d.bowler AS player_name,
    sq.team,
    COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0) AS total_legal_balls,
    COUNT(*) FILTER (WHERE d.runs_total = 0 AND d.extra_wides = 0 AND d.extra_noballs = 0) AS dot_balls,
    ROUND(COUNT(*) FILTER (WHERE d.runs_total = 0 AND d.extra_wides = 0 AND d.extra_noballs = 0)::NUMERIC / NULLIF(COUNT(*) FILTER (WHERE d.extra_wides = 0 AND d.extra_noballs = 0), 0) * 100, 2) AS dot_ball_percentage
FROM fact_delivery d
JOIN dim_squad_2026 sq ON d.bowler = sq.player_name
GROUP BY d.bowler, sq.team;

CREATE OR REPLACE VIEW vw_team_head_to_head AS
SELECT
    team1,
    team2,
    COUNT(*) AS total_matches,
    SUM(CASE WHEN winner = team1 THEN 1 ELSE 0 END) AS team1_wins,
    SUM(CASE WHEN winner = team2 THEN 1 ELSE 0 END) AS team2_wins,
    SUM(CASE WHEN winner IS NULL THEN 1 ELSE 0 END) AS no_result
FROM dim_match
GROUP BY team1, team2;

CREATE OR REPLACE VIEW vw_venue_stats AS
SELECT
    m.venue,
    COUNT(DISTINCT m.match_id) AS total_matches,
    ROUND(AVG(i1.total), 2) AS avg_first_innings_score,
    ROUND(AVG(i2.total), 2) AS avg_second_innings_score
FROM dim_match m
LEFT JOIN (
    SELECT match_id, SUM(runs_total) AS total FROM fact_delivery WHERE inning = 1 GROUP BY match_id
) i1 ON m.match_id = i1.match_id
LEFT JOIN (
    SELECT match_id, SUM(runs_total) AS total FROM fact_delivery WHERE inning = 2 GROUP BY match_id
) i2 ON m.match_id = i2.match_id
GROUP BY m.venue;

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
GROUP BY d.match_id, m.match_date, m.venue, m.team1, m.team2, m.winner, m.toss_winner, m.toss_decision, d.inning;

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
GROUP BY d.match_id, m.match_date, m.venue, d.inning, match_phase;
