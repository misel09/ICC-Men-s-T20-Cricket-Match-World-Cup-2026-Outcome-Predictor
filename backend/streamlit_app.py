import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# ── PAGE CONFIG ──────────────────────────────────────────────
st.set_page_config(
    page_title="T20 WC '26 | Master Analytics",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── GLOBAL CSS ───────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700;800;900&display=swap');
*,html,body{font-family:'Outfit',sans-serif!important;}

/* ── Deep Navy Background ── */
[data-testid="stAppViewContainer"]{
  background: #07091A !important;
}
[data-testid="stAppViewBlock"], .main .block-container{
  background: transparent !important;
  padding-top: 1.5rem;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"]{
  background: #0D1130 !important;
  border-right: 1px solid rgba(56, 189, 248, 0.15);
  min-width: 240px !important;
}
[data-testid="stSidebar"] * { color: #E2E8F0 !important; }
[data-testid="stSidebar"] label { color: #94A3B8 !important; font-size: 0.76rem !important; }

/* Nav buttons */
.nav-btn {
  display: block; width: 100%; padding: 10px 16px; margin: 6px 0;
  border-radius: 8px; font-size: 0.9rem; font-weight: 700;
  cursor: pointer; text-align: left;
  border: 1px solid rgba(255,255,255,0.08);
  background: rgba(255,255,255,0.04);
  color: #CBD5E1 !important;
  transition: all 0.2s ease;
}
.nav-btn:hover { background: rgba(56,189,248,0.1); border-color: rgba(56,189,248,0.35); color: #38BDF8 !important; }
.nav-btn-active { background: rgba(56,189,248,0.15) !important; border-color: rgba(56,189,248,0.5) !important; color: #38BDF8 !important; }

/* ── Tab bar ── */
.stTabs [data-baseweb="tab-list"]{
  background: rgba(13, 17, 48, 0.8);
  border-radius: 10px; padding: 6px 8px; gap: 6px;
  border: 1px solid rgba(56, 189, 248, 0.2);
}
.stTabs [data-baseweb="tab"]{
  border-radius: 8px; padding: 8px 18px;
  color: #64748B !important; font-weight: 600; font-size: 0.88rem;
  background: transparent; transition: all 0.2s ease;
}
.stTabs [data-baseweb="tab"]:hover { color: #CBD5E1 !important; background: rgba(255,255,255,0.04); }
.stTabs [aria-selected="true"] {
  background: rgba(56, 189, 248, 0.15) !important;
  color: #38BDF8 !important;
  border: 1px solid rgba(56, 189, 248, 0.45);
}

/* ── KPI Cards — transparent ── */
.kcard{
  background: transparent;
  border: 1px solid rgba(56, 189, 248, 0.18);
  border-radius: 14px; padding: 22px 16px;
  text-align: center; position: relative; overflow: hidden;
  transition: border-color 0.25s ease;
}
.kcard:hover { border-color: rgba(56, 189, 248, 0.55); }
.kcard::before {
  content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
  background: linear-gradient(90deg, transparent, #38BDF8, #818CF8, transparent);
}
/* KPI VALUE — Sky Blue */
.kval { font-size: 2.2rem; font-weight: 800; color: #38BDF8 !important; line-height: 1.2; }
/* KPI LABEL — Amber/Gold for contrast */
.klbl { font-size: 0.72rem; color: #F59E0B !important; text-transform: uppercase;
        letter-spacing: 1.6px; margin-top: 8px; font-weight: 700; }
.kico { font-size: 1.2rem; margin-bottom: 5px; }

/* ── Section headings — Violet gradient ── */
.sec {
  font-size: 1.05rem; font-weight: 800;
  background: linear-gradient(90deg, #818CF8, #C084FC);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  margin: 18px 0 10px 0;
  border-bottom: 1px solid rgba(129,140,248,0.2); padding-bottom: 6px;
}

/* ── Global text — force visibility ── */
h1, h2, h3, h4, h5, h6 { color: #F1F5F9 !important; }
p, span, div, label, li { color: #CBD5E1 !important; }
.stMarkdown p, .stMarkdown span { color: #CBD5E1 !important; }
strong, b { color: #F1F5F9 !important; }
small, .caption { color: #94A3B8 !important; }

/* ── Dropdowns — High Contrast Styled ── */
div[data-baseweb="select"] > div, 
div[data-baseweb="select"] [data-baseweb="popover"],
.stSelectbox [data-baseweb="select"] {
  background-color: #0b0f1a !important;
  border: 1px solid rgba(56,189,248,0.4) !important;
  color: #ffffff !important;
}
div[role="listbox"], [data-baseweb="popover"] {
  background-color: #0b0f1a !important;
  border: 1px solid rgba(56,189,248,0.5) !important;
}
[data-baseweb="option"] {
  background-color: transparent !important;
  color: # CBD5E1 !important;
}
[data-baseweb="option"]:hover {
  background-color: rgba(56,189,248,0.15) !important;
  color: #38BDF8 !important;
}
/* Force text visibility in select boxes */
.stSelectbox div[data-baseweb="select"] span {
  color: #ffffff !important;
}

/* ── Data table ── */
div[data-testid="stDataFrame"] {
  background: transparent; border-radius: 10px; overflow: hidden;
  border: 1px solid rgba(56, 189, 248, 0.15);
}
th { background-color: rgba(56,189,248,0.08) !important;
     color: #38BDF8 !important; font-weight: 700 !important;
     border-bottom: 1px solid rgba(56,189,248,0.25) !important; }
td { color: #CBD5E1 !important; background: transparent !important; }

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 5px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: rgba(56,189,248,0.4); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: rgba(56,189,248,0.7); }
</style>
""", unsafe_allow_html=True)

# ── THEME HELPERS ────────────────────────────────────────────


COLORS = ["#00f2fe","#f59e0b","#10b981","#ef4444","#8b5cf6",
          "#ec4899","#3b82f6","#a3e635","#fb923c","#e879f9"]
_GRID  = "rgba(255,255,255,0.05)"

def _base():
    return {
        "plot_bgcolor": "rgba(0,0,0,0)",
        "paper_bgcolor": "rgba(0,0,0,0)",
        "font": {"color": "#e2e8f0", "family": "Outfit", "size": 12},
        "legend": {
            "bgcolor": "rgba(11,13,23,.85)",
            "bordercolor": "rgba(0,242,254,.2)",
            "borderwidth": 1
        },
        "hoverlabel": {
            "bgcolor": "rgba(11,13,23,.95)",
            "bordercolor": "#00f2fe",
            "font_size": 13,
            "font_family": "Outfit"
        },
        "margin": {"l": 10, "r": 10, "t": 50, "b": 10}
    }

def theme(fig, grid=True, **kw):
    """Apply dark theme. grid=False for Pie/Treemap/Heatmap."""
    layout_cfg = _base()
    layout_cfg.update(kw)
    fig.update_layout(**layout_cfg)
    if grid:
        fig.update_xaxes(gridcolor=_GRID, zeroline=False, tickfont_size=11)
        fig.update_yaxes(gridcolor=_GRID, zeroline=False, tickfont_size=11)
    return fig

@st.cache_resource
def get_conn():
    """Returns a SQLAlchemy engine."""
    try:
        host = os.getenv("DB_HOST", "").strip()
        port = os.getenv("DB_PORT", "").strip()
        user = os.getenv("DB_USER", "").strip()
        password = os.getenv("DB_PASSWORD", "").strip()
        dbname = os.getenv("DB_NAME", "").strip()
        
        if not all([host, port, user, dbname]):
            return None
            
        # Construct SQLAlchemy DATABASE_URL
        url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        return create_engine(url)
    except Exception:
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def qry(view):
    engine = get_conn()
    if engine is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(f"SELECT * FROM {view}", engine)
        for c in df.columns:
            try:
                df[c] = pd.to_numeric(df[c])
            except (ValueError, TypeError):
                pass
        return df
    except Exception:
        return pd.DataFrame()

def C(df, *names):
    for n in names:
        if n in df.columns:
            return n
    return None

@st.cache_data(ttl=3600, show_spinner=False)
def get_bat_first_stats(ta: str, tb: str):
    """Query dim_match to get bat-first vs bat-second wins for ta vs tb.
    Returns dict: {bat_first_ta, bat_second_ta, bat_first_tb, bat_second_tb, total}
    """
    engine = get_conn()
    if engine is None:
        return None
    try:
        sql = """
            SELECT
                -- team batting first = toss winner chose 'bat',
                --                   OR toss winner chose 'field' (meaning opponent batted first)
                SUM(CASE
                    WHEN toss_decision = 'bat' AND toss_winner = winner THEN 1
                    WHEN toss_decision = 'field' AND toss_winner != winner AND winner IS NOT NULL THEN 1
                    ELSE 0 END) AS bat_first_wins,
                SUM(CASE
                    WHEN toss_decision = 'field' AND toss_winner = winner THEN 1
                    WHEN toss_decision = 'bat' AND toss_winner != winner AND winner IS NOT NULL THEN 1
                    ELSE 0 END) AS bat_second_wins,
                COUNT(*) AS total_matches,
                SUM(CASE WHEN winner IS NULL OR winner = '' OR winner = 'No result' THEN 1 ELSE 0 END) AS no_result
            FROM dim_match
            WHERE
                ((team1 = %(ta)s AND team2 = %(tb)s)
                 OR (team1 = %(tb)s AND team2 = %(ta)s))
                AND winner IS NOT NULL AND winner != 'No result'
        """
        df = pd.read_sql(sql, engine, params={'ta': ta, 'tb': tb})
        if df.empty:
            return None
        row = df.iloc[0]
        return {
            'bat_first_wins' : int(row['bat_first_wins']  or 0),
            'bat_second_wins': int(row['bat_second_wins'] or 0),
            'total_matches'  : int(row['total_matches']   or 0),
            'no_result'      : int(row['no_result']       or 0),
        }
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def get_yearly_h2h_wins(ta: str, tb: str):
    """Year-by-year wins for ta and tb in their H2H matches."""
    engine = get_conn()
    if engine is None:
        return pd.DataFrame()
    try:
        sql = """
            SELECT
                EXTRACT(YEAR FROM match_date)::int AS year,
                winner,
                COUNT(*) AS wins
            FROM dim_match
            WHERE
                ((team1 = %(ta)s AND team2 = %(tb)s)
                 OR (team1 = %(tb)s AND team2 = %(ta)s))
                AND winner IN (%(ta)s, %(tb)s)
            GROUP BY year, winner
            ORDER BY year
        """
        df = pd.read_sql(sql, engine, params={'ta': ta, 'tb': tb})
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_phase_runs():
    """Avg runs per match phase per venue from vw_phase_analysis, split by inning."""
    engine = get_conn()
    if engine is None:
        return pd.DataFrame()
    try:
        sql = """
            SELECT
                venue,
                match_phase,
                inning,
                ROUND(AVG(runs_scored)::numeric, 1) AS avg_runs
            FROM vw_phase_analysis
            WHERE venue IS NOT NULL AND match_phase IS NOT NULL AND inning IN (1, 2)
            GROUP BY venue, match_phase, inning
            ORDER BY venue, match_phase, inning
        """
        return pd.read_sql(sql, engine)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_custom_kpis(venue: str):
    """Direct queries for KPIs to handle venues filtered out of vw_venue_stats."""
    engine = get_conn()
    if engine is None:
        return "N/A", "N/A", "N/A", None
    try:
        avg1 = avg2 = mhst = "N/A"
        bf_pct = None
        
        # 1. Matches Hosted
        m_df = pd.read_sql("SELECT COUNT(*) AS c FROM dim_match WHERE venue = %(v)s", engine, params={'v': venue})
        if not m_df.empty:
            mhst = int(m_df['c'].iloc[0])
            
        # 2. Bat-First Win %
        sql_bf = """
            SELECT
                COUNT(*) AS total,
                SUM(CASE
                    WHEN (toss_decision='bat' AND toss_winner=winner)
                      OR (toss_decision='field' AND toss_winner!=winner
                          AND winner IS NOT NULL AND winner!='No result')
                    THEN 1 ELSE 0 END) AS bf_wins
            FROM dim_match
            WHERE venue = %(v)s
              AND winner IS NOT NULL AND winner != 'No result'
        """
        bf_df = pd.read_sql(sql_bf, engine, params={'v': venue})
        if not bf_df.empty and int(bf_df['total'].iloc[0]) > 0:
            total_m = int(bf_df['total'].iloc[0])
            bf_w = int(bf_df['bf_wins'].iloc[0])
            bf_pct = float(f"{bf_w / total_m * 100:.1f}")

        # 3. Avg Innings (calculated from fact_delivery directly)
        sql_inn = """
            SELECT inning, ROUND(AVG(runs)::numeric, 1) AS avg_score
            FROM (
                SELECT f.match_id, f.inning, SUM(f.runs_total) AS runs
                FROM fact_delivery f
                JOIN dim_match m ON f.match_id = m.match_id
                WHERE m.venue = %(v)s AND f.inning IN (1, 2)
                GROUP BY f.match_id, f.inning
            ) d
            GROUP BY inning
        """
        inn_df = pd.read_sql(sql_inn, engine, params={'v': venue})
        for _, r in inn_df.iterrows():
            if r['inning'] == 1: avg1 = float(r['avg_score'])
            if r['inning'] == 2: avg2 = float(r['avg_score'])
            
        return avg1, avg2, mhst, bf_pct
    except Exception:
        return "N/A", "N/A", "N/A", None

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_highest_totals(venue: str):
    """Query top matches by 1st innings score at a venue."""
    engine = get_conn()
    if engine is None:
        return pd.DataFrame()
    try:
        sql = """
            SELECT 
                m.match_date as date, 
                m.team1 || ' vs ' || m.team2 as teams,
                m.winner, 
                m.win_by_runs,
                m.win_by_wickets,
                inn1.runs as inn1,
                inn2.runs as inn2
            FROM dim_match m
            LEFT JOIN (SELECT match_id, sum(runs_total) as runs FROM fact_delivery WHERE inning = 1 GROUP BY match_id) inn1 ON m.match_id = inn1.match_id
            LEFT JOIN (SELECT match_id, sum(runs_total) as runs FROM fact_delivery WHERE inning = 2 GROUP BY match_id) inn2 ON m.match_id = inn2.match_id
            WHERE m.venue = %(v)s AND inn1.runs IS NOT NULL
            ORDER BY inn1.runs DESC
            LIMIT 5
        """
        return pd.read_sql(sql, engine, params={'v': venue})
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_win_types(venue: str):
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            SELECT 
                SUM(CASE WHEN win_by_runs > 0 THEN 1 ELSE 0 END) as bat_first,
                SUM(CASE WHEN win_by_wickets > 0 THEN 1 ELSE 0 END) as bowl_first
            FROM dim_match
            WHERE venue = %(v)s
        """
        return pd.read_sql(sql, engine, params={'v': venue})
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_innings_scores(venue: str):
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            SELECT f.inning, sum(f.runs_total) as score
            FROM fact_delivery f
            JOIN dim_match m ON f.match_id = m.match_id
            WHERE m.venue = %(v)s AND f.inning IN (1,2)
            GROUP BY m.match_id, f.inning
        """
        return pd.read_sql(sql, engine, params={'v': venue})
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_team_phase_stats(team_name):
    engine = get_conn()
    if engine is None: return pd.DataFrame(), "No Connection"
    try:
        sql = """
            WITH team_innings AS (
                SELECT 
                    match_id,
                    CASE 
                        WHEN (team1 = %(t)s AND ((toss_winner = team1 AND toss_decision = 'bat') OR (toss_winner = team2 AND toss_decision = 'field')))
                          OR (team2 = %(t)s AND ((toss_winner = team2 AND toss_decision = 'bat') OR (toss_winner = team1 AND toss_decision = 'field')))
                        THEN 1
                        ELSE 2
                    END as batting_inn
                FROM dim_match
                WHERE team1 = %(t)s OR team2 = %(t)s
            )
            SELECT 
                CASE 
                    WHEN f.over_number < 6 THEN 'Powerplay'
                    WHEN f.over_number < 15 THEN 'Middle'
                    ELSE 'Death'
                END as phase,
                f.match_id,
                SUM(f.runs_total) as runs,
                COUNT(f.wicket_type) as wickets,
                COUNT(CASE WHEN f.extra_wides=0 AND f.extra_noballs=0 THEN 1 END) as valid_balls
            FROM fact_delivery f
            JOIN team_innings ti ON f.match_id = ti.match_id AND f.inning = ti.batting_inn
            GROUP BY phase, f.match_id
        """
        return pd.read_sql(sql, engine, params={'t': team_name}), None
    except Exception as e:
        return pd.DataFrame(), str(e)

@st.cache_data(ttl=3600, show_spinner=False)
def get_team_over_by_over(team_name):
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            WITH team_innings AS (
                SELECT match_id,
                    CASE WHEN (team1 = %(t)s AND ((toss_winner = team1 AND toss_decision = 'bat') OR (toss_winner = team2 AND toss_decision = 'field')))
                      OR (team2 = %(t)s AND ((toss_winner = team2 AND toss_decision = 'bat') OR (toss_winner = team1 AND toss_decision = 'field')))
                    THEN 1 ELSE 2 END as inn
                FROM dim_match WHERE team1 = %(t)s OR team2 = %(t)s
            )
            SELECT f.over_number + 1 as over_num, SUM(f.runs_total) as total_runs, 
                   COUNT(CASE WHEN f.extra_wides=0 AND f.extra_noballs=0 THEN 1 END) as valid_balls
            FROM fact_delivery f
            JOIN team_innings ti ON f.match_id = ti.match_id AND f.inning = ti.inn
            GROUP BY f.over_number ORDER BY 1
        """
        return pd.read_sql(sql, engine, params={'t': team_name})
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_compare_teams_phase_stats():
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            WITH match_teams AS (
                SELECT match_id, team1 as team, 
                       CASE WHEN (toss_decision = 'bat' AND toss_winner = team1) OR (toss_decision = 'field' AND toss_winner = team2) THEN 1 ELSE 2 END as batting_inn
                FROM dim_match
                UNION ALL
                SELECT match_id, team2 as team, 
                       CASE WHEN (toss_decision = 'bat' AND toss_winner = team2) OR (toss_decision = 'field' AND toss_winner = team1) THEN 1 ELSE 2 END as batting_inn
                FROM dim_match
            )
            SELECT 
                mt.team,
                v.match_phase as phase,
                AVG(v.runs_scored) as avg_runs
            FROM vw_phase_analysis v
            JOIN match_teams mt ON v.match_id = mt.match_id AND v.inning = mt.batting_inn
            GROUP BY mt.team, v.match_phase
        """
        return pd.read_sql(sql, engine)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_all_phase_stats():
    """Aggregated stats for all teams across phases."""
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            WITH match_teams AS (
                SELECT match_id, team1 as team, 
                       CASE WHEN (toss_decision = 'bat' AND toss_winner = team1) OR (toss_decision = 'field' AND toss_winner = team2) THEN 1 ELSE 2 END as batting_inn
                FROM dim_match
                UNION ALL
                SELECT match_id, team2 as team, 
                       CASE WHEN (toss_decision = 'bat' AND toss_winner = team2) OR (toss_decision = 'field' AND toss_winner = team1) THEN 1 ELSE 2 END as batting_inn
                FROM dim_match
            )
            SELECT 
                mt.team,
                v.match_phase,
                AVG(v.runs_scored) as avg_runs
            FROM vw_phase_analysis v
            JOIN match_teams mt ON v.match_id = mt.match_id AND v.inning = mt.batting_inn
            GROUP BY mt.team, v.match_phase
        """
        return pd.read_sql(sql, engine)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_team_over_progression(team_name):
    """Average runs per over (1-20) for the selected team."""
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            WITH team_innings AS (
                SELECT 
                    match_id,
                    CASE 
                        WHEN (team1 = %(t)s AND ((toss_winner = team1 AND toss_decision = 'bat') OR (toss_winner = team2 AND toss_decision = 'field')))
                          OR (team2 = %(t)s AND ((toss_winner = team2 AND toss_decision = 'bat') OR (toss_winner = team1 AND toss_decision = 'field')))
                        THEN 1
                        ELSE 2
                    END as inn
                FROM dim_match
                WHERE team1 = %(t)s OR team2 = %(t)s
            )
            SELECT f.over_number + 1 as over, AVG(over_runs) as avg_runs
            FROM (
                SELECT f.match_id, f.over_number, SUM(f.runs_total) as over_runs
                FROM fact_delivery f
                JOIN team_innings ti ON f.match_id = ti.match_id AND f.inning = ti.inn
                GROUP BY f.match_id, f.over_number
            ) d
            GROUP BY 1 ORDER BY 1
        """
        return pd.read_sql(sql, engine, params={'t': team_name})
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_bowler_stats_vs_opponent(team_name, opponent_name):
    """Head-to-head bowler stats for a specific team vs opponent."""
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            WITH team_bowling_innings AS (
                SELECT 
                    match_id,
                    CASE 
                        WHEN (team1 = %(t)s AND ((toss_winner = team1 AND toss_decision = 'bat') OR (toss_winner = team2 AND toss_decision = 'field')))
                          OR (team2 = %(t)s AND ((toss_winner = team2 AND toss_decision = 'bat') OR (toss_winner = team1 AND toss_decision = 'field')))
                        THEN 2
                        ELSE 1
                    END as bowling_inn
                FROM dim_match
                WHERE (team1 = %(t)s AND team2 = %(o)s) OR (team1 = %(o)s AND team2 = %(t)s)
            )
            SELECT 
                f.bowler as player_name,
                COUNT(f.wicket_type) as wickets,
                ROUND((SUM(f.runs_total)::numeric / COUNT(CASE WHEN f.extra_wides=0 AND f.extra_noballs=0 THEN 1 END)::numeric) * 6, 2) AS economy
            FROM fact_delivery f
            JOIN team_bowling_innings tbi ON f.match_id = tbi.match_id AND f.inning = tbi.bowling_inn
            GROUP BY f.bowler
            HAVING COUNT(CASE WHEN f.extra_wides=0 AND f.extra_noballs=0 THEN 1 END) > 0
            ORDER BY wickets DESC, economy ASC
            LIMIT 15
        """
        return pd.read_sql(sql, engine, params={'t': team_name, 'o': opponent_name})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_team_bowler_stats(team_name):
    """Overall bowler stats for a specific team using vw_bowler_stats."""
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            SELECT 
                player_name,
                wickets,
                economy_rate as economy
            FROM vw_bowler_stats
            WHERE team = %(t)s
            ORDER BY wickets DESC, economy ASC
            LIMIT 15
        """
        return pd.read_sql(sql, engine, params={'t': team_name})
    except Exception:
        return pd.DataFrame()



@st.cache_data(ttl=3600, show_spinner=False)
def get_batting_stats_vs_opponent(team_name, opponent_name):
    """Head-to-head batting stats for a specific team vs opponent."""
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            WITH team_batting_innings AS (
                SELECT 
                    match_id,
                    CASE 
                        WHEN (team1 = %(t)s AND ((toss_winner = team1 AND toss_decision = 'bat') OR (toss_winner = team2 AND toss_decision = 'field')))
                          OR (team2 = %(t)s AND ((toss_winner = team2 AND toss_decision = 'bat') OR (toss_winner = team1 AND toss_decision = 'field')))
                        THEN 1
                        ELSE 2
                    END as batting_inn
                FROM dim_match
                WHERE (team1 = %(t)s AND team2 = %(o)s) OR (team1 = %(o)s AND team2 = %(t)s)
            )
            SELECT 
                f.batter as player_name,
                SUM(f.runs_batter) as total_runs,
                COUNT(CASE WHEN f.wicket_type IS NOT NULL THEN 1 END) as total_outs,
                ROUND((SUM(f.runs_batter)::numeric / COUNT(CASE WHEN f.extra_wides=0 AND f.extra_noballs=0 THEN 1 END)::numeric) * 100, 2) AS strike_rate
            FROM fact_delivery f
            JOIN team_batting_innings tbi ON f.match_id = tbi.match_id AND f.inning = tbi.batting_inn
            GROUP BY f.batter
            HAVING COUNT(CASE WHEN f.extra_wides=0 AND f.extra_noballs=0 THEN 1 END) > 0
            ORDER BY total_runs DESC
            LIMIT 15
        """
        df = pd.read_sql(sql, engine, params={'t': team_name, 'o': opponent_name})
        # Calculate Average: Runs / Outs (handling 0 outs)
        df['average'] = df.apply(lambda r: round(float(r['total_runs'] / max(1, r['total_outs'])), 2), axis=1)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600, show_spinner=False)
def get_team_batting_stats(team_name):
    """Overall batting stats for a specific team using vw_batter_stats."""
    engine = get_conn()
    if engine is None: return pd.DataFrame()
    try:
        sql = """
            SELECT 
                player_name,
                total_runs,
                strike_rate
            FROM vw_batter_stats
            WHERE team = %(t)s
            ORDER BY total_runs DESC
            LIMIT 15
        """
        return pd.read_sql(sql, engine, params={'t': team_name})
    except Exception:
        return pd.DataFrame()


# ── LOAD DATA ────────────────────────────────────────────────
with st.spinner("⚡ Loading live data..."):
    df_bat  = qry("vw_batter_stats")
    df_bowl = qry("vw_bowler_stats")
    df_h2h  = qry("vw_team_head_to_head")
    df_ven  = qry("vw_venue_stats")
    df_pha  = qry("vw_phase_analysis")

db_ok   = not (df_bat.empty and df_bowl.empty and df_h2h.empty)
tbc     = C(df_bat,  "team")
tbcw    = C(df_bowl, "team")
vc      = C(df_ven,  "venue","stadium","ground")

# ── SIDEBAR ──────────────────────────────────────────────────
sc = "#10b981" if db_ok else "#ef4444"
status_label = "DATABASE LIVE" if db_ok else "DB OFFLINE"
status_icon  = "🟢" if db_ok else "🔴"

st.sidebar.markdown(f"""
<div style='text-align:center;padding:16px 0 8px'>
  <div style='font-size:2.2rem;line-height:1'>🏏</div>
  <div style='font-size:1.2rem;font-weight:900;color:#38BDF8;letter-spacing:1.5px;margin-top:6px'>T20 WC '26</div>
  <div style='font-size:0.7rem;color:#64748B;letter-spacing:3px;margin-top:2px'>MASTER ANALYTICS</div>
  <div style='display:inline-block;margin-top:10px;padding:3px 12px;border-radius:20px;
    background:rgba({"16,185,129" if db_ok else "239,68,68"},0.12);
    border:1px solid rgba({"16,185,129" if db_ok else "239,68,68"},0.4);
    font-size:0.7rem;font-weight:700;letter-spacing:1px;color:{sc}'>
    {status_icon} {status_label}
  </div>
</div>
<hr style='border-color:rgba(56,189,248,0.1);margin:12px 0 16px'>
""", unsafe_allow_html=True)

# ── Navigation buttons ──
st.sidebar.markdown("""
<div style='font-size:0.72rem;font-weight:700;color:#64748B;letter-spacing:2px;margin-bottom:8px;padding-left:4px'>NAVIGATION</div>
""", unsafe_allow_html=True)

nav = st.sidebar.radio(
    "Navigation",
    options=["📊  Analytics Dashboard", "🔮  Predictions"],
    label_visibility="collapsed"
)

st.sidebar.markdown("""
<hr style='border-color:rgba(56,189,248,0.1);margin:16px 0'>
<div style='font-size:0.72rem;font-weight:700;color:#64748B;letter-spacing:2px;margin-bottom:8px;padding-left:4px'>FILTERS</div>
""", unsafe_allow_html=True)

all_teams  = sorted(df_bat[tbc].dropna().unique())  if tbc else []
all_venues = sorted(df_ven[vc].dropna().unique())   if vc  else []
sel_teams  = st.sidebar.multiselect("🎯 Teams",  all_teams,  placeholder="All Teams")
sel_venues = st.sidebar.multiselect("🏟️ Venues", all_venues, placeholder="All Venues")

st.sidebar.markdown("""
<div style='font-size:0.65rem;color:#334155;text-align:center;margin-top:20px'>Refreshes every 60 min · PostgreSQL Gold Layer</div>
""", unsafe_allow_html=True)
pn = C(df_bat, "player_name","batter","name")
bn = C(df_bowl,"player_name","bowler","name")

# ── FILTERED FRAMES ──────────────────────────────────────────
bf  = df_bat[df_bat[tbc].isin(sel_teams)]  if (sel_teams and tbc) else df_bat.copy()
bwf = df_bowl[df_bowl[tbcw].isin(sel_teams)] if (sel_teams and tbcw) else df_bowl.copy()
vf  = df_ven[df_ven[vc].isin(sel_venues)]  if (sel_venues and vc) else df_ven.copy()

# ══════════════════════════════════════════════════════════════
# ROUTING — based on sidebar nav selection
# ══════════════════════════════════════════════════════════════
if "🔮" in nav:
    # ─── PREDICTIONS PAGE ───────────────────────────────────────
    import sys as _sys
    _proj = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if _proj not in _sys.path:
        _sys.path.insert(0, _proj)

    @st.cache_resource(show_spinner=False)
    def _load_models():
        from catboost import CatBoostClassifier, CatBoostRegressor
        from feature_extractor import extractor as _fe
        _base = _proj
        _mm = CatBoostClassifier(); _mm.load_model(os.path.join(_base, 'models', 'cricket_match_predictor.cbm'))
        _sm = {}
        for _n in ['total_runs','pp_runs','mid_runs','death_runs','pp_wickets','mid_wickets','death_wickets']:
            _m = CatBoostRegressor(); _m.load_model(os.path.join(_base, 'models', f'{_n}_model.cbm'))
            _sm[_n] = _m
        _pm = CatBoostClassifier(); _pm.load_model(os.path.join(_base, 'models', 'playing11_model (2).cbm'))
        return _fe, _mm, _sm, _pm

    try:
        fe, match_model, score_models, p11_model = _load_models()
        models_ok = True
    except Exception as _e:
        models_ok = False
        st.error(f"⚠️ Could not load models: {_e}")

    # Page header
    st.markdown("""
    <div style='padding:6px 0 4px'>
      <h1 style='font-size:2.3rem;font-weight:900;margin:0;
       background:linear-gradient(90deg,#38BDF8,#818CF8,#F59E0B);
       -webkit-background-clip:text;-webkit-text-fill-color:transparent'>
        🔮 T20 WC '26 Match Predictor
      </h1>
      <p style='color:#94A3B8;font-size:1rem;margin:4px 0 0 2px'>
        AI-powered match outcome, score &amp; playing XI predictor
      </p>
    </div>
    <div style='height:3px;background:linear-gradient(90deg,#38BDF8,#818CF8,transparent);
     border-radius:2px;margin:10px 0 20px'></div>
    """, unsafe_allow_html=True)

    if models_ok:
        # Use teams & venues already loaded from the database
        _teams_list  = all_teams  if all_teams  else ["India","Australia","England","Pakistan","New Zealand","South Africa","West Indies","Sri Lanka","Bangladesh","Afghanistan"]
        _venues_list = all_venues if all_venues else []
        
        # Dataset Status Check
        ds_match = fe.df_match is not None and not fe.df_match.empty
        ds_score = fe.df_score is not None and not fe.df_score.empty
        ds_p11   = fe.df_playing11 is not None and not fe.df_playing11.empty
        
        if not (ds_match or ds_score or ds_p11):
            _err_str = "\n".join(fe.load_errors) if fe.load_errors else "Files not found."
            st.warning(f"⚠️ Historical datasets not loaded.\nLooking at: {fe.match_features_path}\nErrors: {_err_str}")
            if st.button("🔄 Force Reload Datasets"):
                fe.load_data()
                st.rerun()

        # Feature Column lists from training specs
        FW_COLS = ['team1', 'team2', 'venue', 'first_innings_team', 'h2h_win_rate', 'recent_form_t1', 'recent_form_t2',
                   'team1_overall_win_rate', 'team2_overall_win_rate', 'avg_score_venue', 'toss_venue_win_rate',
                   'venue_win_rate_t1', 'venue_win_rate_t2', 't1_top_batsmen_avg', 't2_top_batsmen_avg',
                   't1_top_bowler_econ', 't2_top_bowler_econ', 'batting_diff', 'bowling_diff', 'recent_form_diff',
                   'overall_winrate_diff', 'venue_diff', 'batting_ratio', 'bowling_ratio', 'form_ratio',
                   'winrate_ratio', 'team_strength_diff']

        FS_COLS = ['batting_team', 'bowling_team', 'venue', 'innings', 'toss_winner', 'toss_decision',
                   'venue_avg_runs', 'venue_avg_pp_runs', 'venue_avg_mid_runs', 'venue_avg_death_runs',
                   'venue_avg_pp_wickets', 'venue_avg_mid_wickets', 'venue_avg_death_wickets',
                   'team_avg_runs', 'team_avg_pp_runs', 'team_avg_mid_runs', 'team_avg_death_runs',
                   'opponent_avg_runs_conceded', 'opponent_avg_pp_runs_conceded', 'opponent_avg_mid_runs_conceded', 
                   'opponent_avg_death_runs_conceded', 'team_last5_avg_runs', 'opponent_last5_avg_conceded', 
                   'team_win_rate_vs_opponent', 'attack_vs_defense', 'venue_vs_team', 'recent_vs_average']

        PT = st.tabs(["⚔️ Match Winner", "📊 Score Predictor", "👥 Playing XI"])

        # ──── TAB 1: Match Winner ────
        with PT[0]:
            st.markdown('<div class="sec">⚔️ Match Winner Prediction</div>', unsafe_allow_html=True)
            pc1, pc2, pc3 = st.columns(3)
            with pc1:
                mw_t1 = st.selectbox("🔵 Team 1", _teams_list, key="mw_t1")
            with pc2:
                mw_t2 = st.selectbox("🔴 Team 2", [t for t in _teams_list if t != mw_t1], key="mw_t2")
            with pc3:
                mw_v = st.selectbox("🏟️ Venue", _venues_list, key="mw_v")
            if st.button("🔮 Predict Winner", key="btn_mw", width='stretch'):
                if not (mw_t1 and mw_t2 and mw_v):
                    st.warning("Please select Team 1, Team 2, and Venue.")
                else:
                    with st.spinner("Analysing match conditions..."):
                        try:
                            f_dict = fe.get_match_features(mw_t1, mw_t2, mw_v)
                            
                            # Gradio EXPECTED_FEATURES List
                            mw_cols = [
                                'team1', 'team2', 'venue', 'first_innings_team', 
                                'h2h_win_rate', 'recent_form_t1', 'recent_form_t2', 
                                'team1_overall_win_rate', 'team2_overall_win_rate', 
                                'avg_score_venue', 'toss_venue_win_rate', 
                                'venue_win_rate_t1', 'venue_win_rate_t2', 
                                't1_top_batsmen_avg', 't2_top_batsmen_avg', 
                                't1_top_bowler_econ', 't2_top_bowler_econ', 
                                'batting_diff', 'bowling_diff', 'recent_form_diff', 
                                'overall_winrate_diff', 'venue_diff', 'batting_ratio', 
                                'bowling_ratio', 'form_ratio', 'winrate_ratio', 
                                'team_strength_diff'
                            ]
                            
                            # Construction matching Gradio's logic
                            row_vals = [str(f_dict.get(c, "")) if c in ['team1', 'team2', 'venue', 'first_innings_team'] else float(f_dict.get(c, 0.0)) for c in mw_cols]
                            df_mw = pd.DataFrame([row_vals], columns=mw_cols)
                            
                            pred_proba = match_model.predict_proba(df_mw)[0]
                            # match_model.classes_ is likely [0, 1]. In training, 1 = Team 1 won.
                            # So index 1 is mw_t1, index 0 is mw_t2.
                            winner_idx = np.argmax(pred_proba)
                            winner = mw_t1 if winner_idx == 1 else mw_t2
                            conf = int(pred_proba[winner_idx] * 100)
                            
                            p_t1 = int(pred_proba[1] * 100)
                            p_t2 = int(pred_proba[0] * 100)
                            
                            st.markdown(f"""
                            <div style='margin-top:20px;padding:28px;border-radius:16px;
                              border:1px solid rgba(56,189,248,0.35);text-align:center'>
                              <div style='font-size:0.75rem;color:#64748B;letter-spacing:2px;text-transform:uppercase'>Predicted Winner</div>
                              <div style='font-size:3rem;font-weight:900;color:#38BDF8;margin:10px 0'>{winner}</div>
                              <div style='font-size:1.1rem;color:#F59E0B;font-weight:700'>{conf}% Confidence</div>
                            </div>
                            """, unsafe_allow_html=True)
                            st.markdown('<div style="height:14px"></div>', unsafe_allow_html=True)
                            ra, rb = st.columns(2)
                            with ra: st.markdown(f'<div class="kcard"><div class="kval">{p_t1}%</div><div class="klbl">🔵 {mw_t1} Win Prob</div></div>', unsafe_allow_html=True)
                            with rb: st.markdown(f'<div class="kcard"><div class="kval">{p_t2}%</div><div class="klbl">🔴 {mw_t2} Win Prob</div></div>', unsafe_allow_html=True)
                        except Exception as _ex:
                            st.error(f"Winner Prediction error: {_ex}")
                            import traceback
                            st.code(traceback.format_exc())

        # ──── TAB 2: Score Predictor ────
        with PT[1]:
            st.markdown('<div class="sec">📊 Innings Score Prediction</div>', unsafe_allow_html=True)
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                sc_t1 = st.selectbox("🏏 Batting Team", _teams_list, key="sc_t1")
            with sc2:
                sc_t2 = st.selectbox("🎯 Bowling Team", [t for t in _teams_list if t != sc_t1], key="sc_t2")
            with sc3:
                sc_v = st.selectbox("🏟️ Venue", _venues_list, key="sc_v")
            if st.button("🔮 Predict Score", key="btn_sc", width='stretch'):
                if not (sc_t1 and sc_t2 and sc_v):
                    st.warning("Please select Batting Team, Bowling Team, and Venue.")
                else:
                    with st.spinner("Calculating innings score..."):
                        try:
                            f_sc = fe.get_score_features(sc_t1, sc_t2, sc_v)
                            
                            # Gradio SCORE_COLUMNS list
                            sc_cols = ['batting_team', 'bowling_team', 'venue', 'innings', 'toss_winner', 'toss_decision',
                                            'venue_avg_runs', 'venue_avg_pp_runs', 'venue_avg_mid_runs', 'venue_avg_death_runs',
                                            'venue_avg_pp_wickets', 'venue_avg_mid_wickets', 'venue_avg_death_wickets',
                                            'team_avg_runs', 'team_avg_pp_runs', 'team_avg_mid_runs', 'team_avg_death_runs',
                                            'opponent_avg_runs_conceded', 'opponent_avg_pp_runs_conceded', 'opponent_avg_mid_runs_conceded', 
                                            'opponent_avg_death_runs_conceded', 'team_last5_avg_runs', 'opponent_last5_avg_conceded', 
                                            'team_win_rate_vs_opponent', 'attack_vs_defense', 'venue_vs_team', 'recent_vs_average']
                            
                            cat_sc = ['batting_team', 'bowling_team', 'venue', 'toss_winner', 'toss_decision']
                            row_vals = [str(f_sc.get(c, "")) if c in cat_sc else float(f_sc.get(c, 0.0)) for c in sc_cols]
                            df_sc = pd.DataFrame([row_vals], columns=sc_cols)
                            
                            preds = {}
                            for name, model in score_models.items():
                                pred = model.predict(df_sc)[0]
                                preds[name] = int(round(float(pred)))
                                
                            st.markdown('<div style="height:14px"></div>', unsafe_allow_html=True)
                            run_cols = st.columns(4)
                            for col_, (lbl_, val_, clr_) in zip(run_cols, [
                                ("🏏 Total Runs",   preds['total_runs'],   "#38BDF8"),
                                ("⚡ Powerplay",    preds['pp_runs'],      "#818CF8"),
                                ("🔄 Middle Overs", preds['mid_runs'],     "#F59E0B"),
                                ("💥 Death Overs",  preds['death_runs'],   "#10B981"),
                            ]):
                                with col_:
                                    st.markdown(f'<div class="kcard"><div class="kval" style="color:{clr_}">{val_}</div><div class="klbl">{lbl_}</div></div>', unsafe_allow_html=True)
                            st.markdown('<div style="height:10px"></div>', unsafe_allow_html=True)
                            wkt_cols = st.columns(3)
                            for col_, (lbl_, val_) in zip(wkt_cols, [
                                ("⚡ PP Wickets",   preds['pp_wickets']),
                                ("🔄 Mid Wickets",  preds['mid_wickets']),
                                ("💥 Death Wkts",   preds['death_wickets']),
                            ]):
                                with col_:
                                    st.markdown(f'<div class="kcard"><div class="kval">{val_}</div><div class="klbl">{lbl_}</div></div>', unsafe_allow_html=True)
                        except Exception as _ex:
                            st.error(f"Score Prediction error: {_ex}")
                            import traceback
                            st.code(traceback.format_exc())

        # ──── TAB 3: Playing XI ────
        with PT[2]:
            st.markdown('<div class="sec">👥 Playing XI Prediction</div>', unsafe_allow_html=True)
            p1, p2, p3 = st.columns(3)
            
            # Use teams/venues specific to the Playing XI dataset to ensure codes exist
            _p11_teams = sorted(fe.team_reverse.keys()) if fe.team_reverse else _teams_list
            _p11_opps  = sorted(fe.opponent_reverse.keys()) if fe.opponent_reverse else _teams_list
            _p11_vens  = sorted(fe.venue_reverse.keys()) if fe.venue_reverse else _venues_list
            
            with p1:
                p11_t = st.selectbox("🏏 Team",     _p11_teams, key="p11_t")
            with p2:
                p11_o = st.selectbox("🎯 Opponent", [t for t in _p11_opps if t != p11_t], key="p11_o")
            with p3:
                p11_v = st.selectbox("🏟️ Venue", _p11_vens, key="p11_v")

            if st.button("🔮 Predict Playing XI", key="btn_p11", width='stretch'):
                with st.spinner("Selecting best 11 players..."):
                    try:
                        team_players, team_map, role_map = fe.get_playing11_features(p11_t, p11_o, p11_v)
                        if team_players.empty:
                             st.warning("No data found for the selected team.")
                        else:
                            FEATURES_P11 = [
                                'team','opponent','venue','player_role','designation',
                                'batting_average', 'strike_rate', 'bowling_economy',
                                'career_total_runs', 'career_wickets',
                                'runs_last_5_matches', 'wickets_last_5_matches',
                                'runs_at_venue', 'wickets_at_venue',
                                'runs_vs_opponent', 'batting_avg_vs_opponent', 'wickets_vs_opponent',
                                'bowling_econ_vs_opponent', 'wickets_last5_vs_opponent',
                                'selection_rate'
                            ]
                            
                            team_players['prob'] = p11_model.predict_proba(team_players[FEATURES_P11])[:,1]
                            players = team_players.groupby(['player_name','team','player_role'])['prob'].mean().reset_index()
                            players['player_role'] = players['player_role'].map(role_map)
                            
                            playing_xi = players.sort_values('prob', ascending=False).head(11).copy()
                            playing_xi['Selection %'] = (playing_xi['prob'] * 100).round(1).astype(str) + "%"
                            playing_xi = playing_xi.reset_index(drop=True)
                            playing_xi.index += 1
                            
                            st.markdown(f"<div style='margin:12px 0 8px;font-size:0.9rem;color:#64748B'>Best XI for <b style='color:#38BDF8'>{p11_t}</b> vs {p11_o} at {p11_v}</div>", unsafe_allow_html=True)
                            st.dataframe(playing_xi[['player_name', 'player_role', 'Selection %']].rename(columns={'player_name':'Player','player_role':'Role'}), width='stretch')
                    except Exception as _ex:
                        st.error(f"Prediction error: {_ex}")

    st.stop()  # Halt — do not render analytics tabs

else:
    # ─── ANALYTICS DASHBOARD ────────────────────────────────────
    # ── HEADER ──────────────────────────────────────────────────
    st.markdown("""
    <div style='padding:6px 0 4px'>
      <h1 style='font-size:2.3rem;font-weight:900;margin:0;
       background:linear-gradient(90deg,#00f2fe,#4facfe,#f59e0b);
       -webkit-background-clip:text;-webkit-text-fill-color:transparent'>
        🏏 T20 WC '26 Master Analytics
      </h1>
      <p style='color:#94A3B8;font-size:1rem;margin:4px 0 0 2px;font-weight:500'>
        Live Intelligence · PostgreSQL Gold Layer · ICC Men's T20 World Cup 2026
      </p>
    </div>
    <div style='height:4px;background:linear-gradient(90deg,#00f2fe,#4facfe,transparent);
     border-radius:2px;margin:10px 0 18px'></div>
    """, unsafe_allow_html=True)

    # Column refs needed by tabs below
    rc  = C(bf,  "total_runs","runs")
    wkc = C(bwf, "wickets","total_wickets")
    src = C(bf,  "strike_rate")
    eco = C(bwf, "economy_rate","economy")

    # ── TABS ─────────────────────────────────────────────────────
    T = st.tabs(["⚔️ Head-to-Head","🏏 Batting","🎯 Bowling",
                 "🏟️ Venues","⭐ Impact","🔄 Phase Analysis"])


# ══════════════════════════════════════════════════════════════
# TAB 1 — HEAD-TO-HEAD
# ══════════════════════════════════════════════════════════════
with T[0]:
    st.markdown('<div class="sec">Team Head-to-Head Intelligence</div>', unsafe_allow_html=True)
    if df_h2h.empty:
        st.warning("⚠️ `vw_team_head_to_head` has no data.")
    else:
        tc1s = [c for c in df_h2h.columns if "team" in c.lower()]
        wc1s = [c for c in df_h2h.columns if "win"  in c.lower()]
        if len(tc1s) >= 2:
            tc1, tc2 = tc1s[0], tc1s[1]
            all_t = sorted(set(df_h2h[tc1].dropna().tolist() + df_h2h[tc2].dropna().tolist()))
            c1, c2 = st.columns(2)
            with c1: ta = st.selectbox("🔵 Team 1", all_t, 0, key="h1")
            with c2: tb = st.selectbox("🔴 Team 2", all_t, min(1,len(all_t)-1), key="h2")

            if ta != tb:
                mask = ((df_h2h[tc1]==ta)&(df_h2h[tc2]==tb)) | \
                       ((df_h2h[tc1]==tb)&(df_h2h[tc2]==ta))
                m = df_h2h[mask]
                if m.empty:
                    st.info(f"No matchups: **{ta}** vs **{tb}**")
                elif len(wc1s) >= 2:
                    row  = m.iloc[0]
                    flip = (row[tc1] == tb)
                    wa   = int(row[wc1s[1] if flip else wc1s[0]])
                    wb   = int(row[wc1s[0] if flip else wc1s[1]])
                    tot  = wa + wb

                    for col_obj, (lbl, val) in zip(
                        st.columns(4),
                        [(f"🔵 {ta} Wins", wa),
                         (f"{float(wa/tot*100) if tot else 0:.1f}% Win Rate", ta),
                         (f"🔴 {tb} Wins", wb),
                         ("Total Games", tot)]
                    ):
                        with col_obj:
                            st.markdown(
                                f'<div class="kcard"><div class="kval">{val}</div>'
                                f'<div class="klbl">{lbl}</div></div>',
                                unsafe_allow_html=True)

                    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)
                    c1, c2 = st.columns(2)

                    # Donut pie
                    with c1:
                        fig = go.Figure(go.Pie(
                            labels=[ta, tb], values=[wa, wb], hole=0.6,
                            pull=[0.04, 0],
                            marker={"colors": ["#00f2fe","#ef4444"],
                                    "line": {"color": "#0b0d17", "width": 3}}
                        ))
                        theme(fig, grid=False,
                              title={"text": "Win Distribution", "x": 0.5, "font_size": 14})
                        fig.update_traces(textfont_size=13)
                        st.plotly_chart(fig, width='stretch')

                    # Bat First vs Bat Second wins donut (specific to this matchup)
                    with c2:
                        bfs = get_bat_first_stats(ta, tb)
                        if bfs and (bfs['bat_first_wins'] + bfs['bat_second_wins']) > 0:
                            bf_wins  = bfs['bat_first_wins']
                            bs_wins  = bfs['bat_second_wins']
                            tot_dec  = bf_wins + bs_wins
                            bf_pct   = round(bf_wins / tot_dec * 100, 1)
                            bs_pct   = round(bs_wins / tot_dec * 100, 1)

                            fig2 = go.Figure(go.Pie(
                                labels=["🏏 Batting First", "🏃 Batting Second"],
                                values=[bf_wins, bs_wins],
                                hole=0.62,
                                pull=[0.04, 0],
                                marker={
                                    "colors": ["#00f2fe", "#f59e0b"],
                                    "line": {"color": "#0b0d17", "width": 3}
                                },
                                textinfo="label+value",
                                textfont_size=12,
                                hovertemplate=(
                                    "<b>%{label}</b><br>"
                                    "Wins: %{value}<br>"
                                    "Share: %{percent}<extra></extra>"
                                )
                            ))
                            fig2.add_annotation(
                                text=(
                                    f"<b>{bfs['total_matches']}</b><br>"
                                    f"<span style='font-size:10px'>Total<br>Matches</span>"
                                ),
                                x=0.5, y=0.5, showarrow=False,
                                font={"size": 15, "color": "#e2e8f0", "family": "Outfit"}
                            )
                            theme(fig2, grid=False,
                                  title={
                                      "text": f"Chase vs Defend — {ta} vs {tb}",
                                      "x": 0.5, "font_size": 13
                                  })
                            st.plotly_chart(fig2, width='stretch')
                            # Mini stat line below the chart
                            st.markdown(
                                f"<div style='text-align:center;font-size:.75rem;color:#ffffff;margin-top:-8px'>"
                                f"🏏 Bat First: <b style='color:#00f2fe'>{bf_wins} wins ({bf_pct}%)</b> &nbsp;|&nbsp; "
                                f"🏃 Bat Second: <b style='color:#f59e0b'>{bs_wins} wins ({bs_pct}%)</b>"
                                f"</div>",
                                unsafe_allow_html=True
                            )
                        else:
                            st.info(f"No bat-first/bat-second data found for **{ta}** vs **{tb}** in dim_match.")

                    # ── Matches Won Over Year — grouped bar chart ─────────
                    st.markdown(
                        f'<div class="sec" style="font-size:1rem;margin-top:8px">'
                        f'📅 Matches Won Over Year — {ta} vs {tb}</div>',
                        unsafe_allow_html=True
                    )

                    df_yearly = get_yearly_h2h_wins(ta, tb)
                    if df_yearly.empty:
                        st.info(f"No year-by-year data found for **{ta}** vs **{tb}**.")
                    else:
                        # Pivot so each team is a column keyed by year
                        piv = df_yearly.pivot_table(
                            index="year", columns="winner", values="wins", fill_value=0
                        ).reset_index()
                        piv["year"] = piv["year"].astype(str)

                        # Ensure both team columns exist (one may have 0 wins in all years)
                        for t in [ta, tb]:
                            if t not in piv.columns:
                                piv[t] = 0

                        fig_yr = go.Figure([
                            go.Bar(
                                name=ta,
                                x=piv["year"],
                                y=piv[ta],
                                marker_color="#00f2fe",
                                marker_line_width=0,
                                text=piv[ta],
                                textposition="outside",
                                textfont={"size": 11, "color": "#00f2fe"},
                                hovertemplate=(
                                    f"<b>{ta}</b><br>"
                                    "Year: <b>%{x}</b><br>"
                                    "Wins: <b>%{y}</b>"
                                    "<extra></extra>"
                                )
                            ),
                            go.Bar(
                                name=tb,
                                x=piv["year"],
                                y=piv[tb],
                                marker_color="#ef4444",
                                marker_line_width=0,
                                text=piv[tb],
                                textposition="outside",
                                textfont={"size": 11, "color": "#ef4444"},
                                hovertemplate=(
                                    f"<b>{tb}</b><br>"
                                    "Year: <b>%{x}</b><br>"
                                    "Wins: <b>%{y}</b>"
                                    "<extra></extra>"
                                )
                            ),
                        ])
                        theme(
                            fig_yr,
                            barmode="group",
                            title=f"Matches Won Over Year — {ta} vs {tb}",
                            xaxis_title="Year",
                            yaxis_title="Matches Won",
                            height=400,
                            xaxis={
                                "type": "category",
                                "tickangle": -30,
                                "tickfont": {"size": 11}
                            },
                            yaxis={"dtick": 1}
                        )
                        st.plotly_chart(fig_yr, width='stretch')

                        # Quick totals below
                        ta_total = int(piv[ta].sum())
                        tb_total = int(piv[tb].sum())
                        c_ya, c_yb = st.columns(2)
                        with c_ya:
                            st.markdown(
                                f"<div class='kcard'>"
                                f"<div class='kval' style='color:#00f2fe'>{ta_total}</div>"
                                f"<div class='klbl'>{ta} — Total H2H Wins</div>"
                                f"</div>",
                                unsafe_allow_html=True
                            )
                        with c_yb:
                            st.markdown(
                                f"<div class='kcard'>"
                                f"<div class='kval' style='color:#ef4444'>{tb_total}</div>"
                                f"<div class='klbl'>{tb} — Total H2H Wins</div>"
                                f"</div>",
                                unsafe_allow_html=True
                            )
        else:
            st.info(f"H2H columns: {df_h2h.columns.tolist()}")

# ══════════════════════════════════════════════════════════════
# TAB 2 — BATTING
# ══════════════════════════════════════════════════════════════
with T[1]:
    st.markdown('<div class="sec">🏏 Batting Powerhouse Analytics</div>', unsafe_allow_html=True)
    
    if not all_teams:
        st.warning("⚠️ No team data available. Check database connection.")
    else:
        bt1, bt2 = st.columns(2)
        with bt1:
            sel_bat_team = st.selectbox("🏏 Select Team", all_teams, index=0, key="bat_team_sel")
        with bt2:
            sel_opp_team_bat = st.selectbox("🎯 Select Opponent (for H2H)", ["None"] + [t for t in all_teams if t != sel_bat_team], index=0, key="bat_opp_sel")

        st.markdown('<div style="height:20px"></div>', unsafe_allow_html=True)

        # ── OVERALL TEAM BATTING STATS ──
        st.markdown(f'<div style="font-size:1.1rem; font-weight:700; color:#ffffff; margin-bottom:10px;">📊 Overall Performance: {sel_bat_team} Batters</div>', unsafe_allow_html=True)
        
        df_bat_overall = get_team_batting_stats(sel_bat_team)
        if df_bat_overall.empty:
            st.info(f"No batting data found for **{sel_bat_team}**.")
        else:
            fig_ba = go.Figure()
            
            # Runs trace
            fig_ba.add_trace(go.Bar(
                name="Runs",
                x=df_bat_overall['player_name'],
                y=df_bat_overall['total_runs'],
                marker_color="#00e5d1",
                marker_line_width=0,
                text=df_bat_overall['total_runs'],
                textposition="outside",
                textfont=dict(color="#00e5d1", size=10),
                hovertemplate="<b>%{x}</b><br>Runs: <b>%{y}</b><extra></extra>"
            ))
            
            # Strike Rate trace
            fig_ba.add_trace(go.Bar(
                name="Strike Rate",
                x=df_bat_overall['player_name'],
                y=df_bat_overall['strike_rate'],
                marker_color="#ec4899",
                marker_line_width=0,
                text=df_bat_overall['strike_rate'].round(0),
                textposition="outside",
                textfont=dict(color="#ec4899", size=10),
                hovertemplate="<b>%{x}</b><br>SR: <b>%{y}</b><extra></extra>"
            ))
            
            fig_ba.update_layout(
                barmode="group",
                xaxis=dict(title="", tickangle=-45, tickfont=dict(color="#ffffff", size=10)),
                yaxis=dict(title="Stats", showgrid=True, gridcolor="rgba(255,255,255,0.05)", zeroline=False),
                height=500,
                margin=dict(t=30, b=80, l=10, r=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="#ffffff"))
            )
            st.plotly_chart(fig_ba, width='stretch')

        # ── HEAD-TO-HEAD BATTING STATS ──
        if sel_opp_team_bat != "None":
            st.markdown('<div style="height:40px"></div>', unsafe_allow_html=True)
            st.markdown(f'<div style="font-size:1.1rem; font-weight:700; color:#ffffff; margin-bottom:10px;">⚔️ H2H Performance: {sel_bat_team} vs {sel_opp_team_bat}</div>', unsafe_allow_html=True)
            
            df_bat_h2h = get_batting_stats_vs_opponent(sel_bat_team, sel_opp_team_bat)
            if df_bat_h2h.empty:
                st.info(f"No specific head-to-head records found for **{sel_bat_team}** batters against **{sel_opp_team_bat}**.")
            else:
                fig_bh = go.Figure()
                
                # Runs trace
                fig_bh.add_trace(go.Bar(
                    name="H2H Runs",
                    x=df_bat_h2h['player_name'],
                    y=df_bat_h2h['total_runs'],
                    marker_color="#00e5d1",
                    marker_line_width=0,
                    text=df_bat_h2h['total_runs'],
                    textposition="outside",
                    textfont=dict(color="#00e5d1", size=10),
                    hovertemplate="<b>%{x} vs "+sel_opp_team_bat+"</b><br>Runs: <b>%{y}</b><extra></extra>"
                ))
                
                # Strike Rate trace
                fig_bh.add_trace(go.Bar(
                    name="H2H SR",
                    x=df_bat_h2h['player_name'],
                    y=df_bat_h2h['strike_rate'],
                    marker_color="#ec4899",
                    marker_line_width=0,
                    text=df_bat_h2h['strike_rate'].round(0),
                    textposition="outside",
                    textfont=dict(color="#ec4899", size=10),
                    hovertemplate="<b>%{x} vs "+sel_opp_team_bat+"</b><br>SR: <b>%{y}</b><extra></extra>"
                ))
                
                fig_bh.update_layout(
                    barmode="group",
                    xaxis=dict(title="", tickangle=-45, tickfont=dict(color="#ffffff", size=10)),
                    yaxis=dict(title="Stats", showgrid=True, gridcolor="rgba(255,255,255,0.05)", zeroline=False),
                    height=500,
                    margin=dict(t=30, b=80, l=10, r=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="#ffffff"))
                )
                st.plotly_chart(fig_bh, width='stretch')

# ══════════════════════════════════════════════════════════════
# TAB 3 — BOWLING
# ══════════════════════════════════════════════════════════════
with T[2]:
    st.markdown('<div class="sec">🎯 Bowler Intelligence Command</div>', unsafe_allow_html=True)
    
    if not all_teams:
        st.warning("⚠️ No team data available. Check database connection.")
    else:
        bc1, bc2 = st.columns(2)
        with bc1:
            sel_bowl_team = st.selectbox("🏏 Select Team", all_teams, index=0, key="bowl_team_sel")
        with bc2:
            sel_opp_team = st.selectbox("🎯 Select Opponent (for H2H)", ["None"] + [t for t in all_teams if t != sel_bowl_team], index=0, key="bowl_opp_sel")

        st.markdown('<div style="height:20px"></div>', unsafe_allow_html=True)

        # ── OVERALL TEAM BOWLER STATS ──
        st.markdown(f'<div style="font-size:1.1rem; font-weight:700; color:#ffffff; margin-bottom:10px;">📊 Overall Performance: {sel_bowl_team} Bowlers</div>', unsafe_allow_html=True)
        
        df_bowl_overall = get_team_bowler_stats(sel_bowl_team)
        if df_bowl_overall.empty:
            st.info(f"No bowling data found for **{sel_bowl_team}**.")
        else:
            fig_bo = go.Figure()
            
            # Wickets trace
            fig_bo.add_trace(go.Bar(
                name="Wickets",
                x=df_bowl_overall['player_name'],
                y=df_bowl_overall['wickets'],
                marker_color="#00e5d1",
                marker_line_width=0,
                text=df_bowl_overall['wickets'],
                textposition="outside",
                textfont=dict(color="#00e5d1", size=10),
                hovertemplate="<b>%{x}</b><br>Wickets: <b>%{y}</b><extra></extra>"
            ))
            
            # Economy trace
            fig_bo.add_trace(go.Bar(
                name="Economy",
                x=df_bowl_overall['player_name'],
                y=df_bowl_overall['economy'],
                marker_color="#f5c518",
                marker_line_width=0,
                text=df_bowl_overall['economy'],
                textposition="outside",
                textfont=dict(color="#f5c518", size=10),
                hovertemplate="<b>%{x}</b><br>Economy: <b>%{y}</b><extra></extra>"
            ))
            
            fig_bo.update_layout(
                barmode="group",
                xaxis=dict(title="", tickangle=-45, tickfont=dict(color="#ffffff", size=10)),
                yaxis=dict(title="Stats", range=[0, max(df_bowl_overall['wickets'].max(), df_bowl_overall['economy'].max()) * 1.2],
                           showgrid=True, gridcolor="rgba(255,255,255,0.05)", zeroline=False),
                height=450,
                margin=dict(t=30, b=80, l=10, r=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="#ffffff"))
            )
            st.plotly_chart(fig_bo, width='stretch')

        # ── HEAD-TO-HEAD BOWLER STATS ──
        if sel_opp_team != "None":
            st.markdown('<div style="height:40px"></div>', unsafe_allow_html=True)
            st.markdown(f'<div style="font-size:1.1rem; font-weight:700; color:#ffffff; margin-bottom:10px;">⚔️ H2H Performance: {sel_bowl_team} vs {sel_opp_team}</div>', unsafe_allow_html=True)
            
            df_bowl_h2h = get_bowler_stats_vs_opponent(sel_bowl_team, sel_opp_team)
            if df_bowl_h2h.empty:
                st.info(f"No specific head-to-head records found for **{sel_bowl_team}** bowlers against **{sel_opp_team}**.")
            else:
                fig_bh = go.Figure()
                
                # Wickets trace
                fig_bh.add_trace(go.Bar(
                    name="H2H Wickets",
                    x=df_bowl_h2h['player_name'],
                    y=df_bowl_h2h['wickets'],
                    marker_color="#00e5d1",
                    marker_line_width=0,
                    text=df_bowl_h2h['wickets'],
                    textposition="outside",
                    textfont=dict(color="#00e5d1", size=10),
                    hovertemplate="<b>%{x} vs "+sel_opp_team+"</b><br>Wickets: <b>%{y}</b><extra></extra>"
                ))
                
                # Economy trace
                fig_bh.add_trace(go.Bar(
                    name="H2H Economy",
                    x=df_bowl_h2h['player_name'],
                    y=df_bowl_h2h['economy'],
                    marker_color="#f5c518",
                    marker_line_width=0,
                    text=df_bowl_h2h['economy'],
                    textposition="outside",
                    textfont=dict(color="#f5c518", size=10),
                    hovertemplate="<b>%{x} vs "+sel_opp_team+"</b><br>Economy: <b>%{y}</b><extra></extra>"
                ))
                
                fig_bh.update_layout(
                    barmode="group",
                    xaxis=dict(title="", tickangle=-45, tickfont=dict(color="#ffffff", size=10)),
                    yaxis=dict(title="Stats", range=[0, max(df_bowl_h2h['wickets'].max(), df_bowl_h2h['economy'].max()) * 1.2],
                               showgrid=True, gridcolor="rgba(255,255,255,0.05)", zeroline=False),
                    height=450,
                    margin=dict(t=30, b=80, l=10, r=10),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(color="#ffffff"))
                )
                st.plotly_chart(fig_bh, width='stretch')

# ══════════════════════════════════════════════════════════════
# TAB 4 — VENUES
# ══════════════════════════════════════════════════════════════
with T[3]:
    st.markdown('<div class="sec" style="color:#f5c518">Venue Analytics Hub</div>', unsafe_allow_html=True)
    if vf.empty:
        st.warning("⚠️ No venue data.")
    else:
        vc4  = C(vf, "venue","stadium","ground")
        fi4  = C(vf, "avg_first_innings_score","first_innings_avg","avg_score")
        si4  = C(vf, "avg_second_innings_score","second_innings_avg")
        mpc4 = C(vf, "matches_played","matches","total_matches")

        if vc4 and fi4:

            # ── Venue Phase Runs chart ───────────────────────
            st.markdown(
                '<div class="sec" style="font-size:1rem;margin-top:8px">'
                '🎚️ Avg Runs by Match Phase — Select a Venue</div>',
                unsafe_allow_html=True
            )
            df_vph = get_venue_phase_runs()
            if df_vph.empty:
                st.info("⚠️ Phase data not available.")
            else:
                venues_list = sorted(df_vph["venue"].dropna().unique().tolist())
                sel_venue = st.selectbox(
                    "🏟️ Choose a Venue",
                    options=venues_list,
                    index=0,
                    key="venue_phase_sel"
                )

                # ── 4 KPI Cards for the selected venue ────────────────
                # Using our custom query specifically for this venue so it never shows N/A
                avg1, avg2, mhst, bf_pct = get_venue_custom_kpis(sel_venue)
                
                bf_str = f"{bf_pct}%" if bf_pct is not None else "N/A"
                bf_arrow = "▲ favors bat-first" if (bf_pct or 0) >= 50 else "▼ favors chase"
                bf_str = f"{bf_pct}%" if bf_pct is not None else "N/A"
                bf_arrow = "▲ favors bat-first" if (bf_pct or 0) >= 50 else "▼ favors chase"

                _card_css = (
                    "background:#1a2332;"
                    "border:1px solid #2a3548;"
                    "border-radius:12px;"
                    "padding:14px 16px 12px;"
                    "min-height:100px;"
                )
                kc1, kc2, kc3, kc4 = st.columns(4)
                with kc1:
                    st.markdown(
                        f"<div style='{_card_css}'>"
                        f"<div style='display:flex;justify-content:space-between;align-items:center'>"
                        f"<span style='font-size:.65rem;color:#ffffff;letter-spacing:.07em;text-transform:uppercase'>Avg 1st Innings</span>"
                        f"<span style='font-size:.9rem;opacity:.35'>📊</span></div>"
                        f"<div style='font-size:2rem;font-weight:800;color:#00e5d1;line-height:1.2;margin-top:6px'>{avg1}</div>"
                        f"<div style='font-size:.7rem;color:#00e5d1;opacity:.7'>runs avg</div>"
                        f"</div>", unsafe_allow_html=True
                    )
                with kc2:
                    st.markdown(
                        f"<div style='{_card_css}'>"
                        f"<div style='display:flex;justify-content:space-between;align-items:center'>"
                        f"<span style='font-size:.65rem;color:#ffffff;letter-spacing:.07em;text-transform:uppercase'>Avg 2nd Innings</span>"
                        f"<span style='font-size:.9rem;opacity:.35'>📈</span></div>"
                        f"<div style='font-size:2rem;font-weight:800;color:#f5c518;line-height:1.2;margin-top:6px'>{avg2}</div>"
                        f"<div style='font-size:.7rem;color:#f5c518;opacity:.7'>runs avg</div>"
                        f"</div>", unsafe_allow_html=True
                    )
                with kc3:
                    st.markdown(
                        f"<div style='{_card_css}'>"
                        f"<div style='display:flex;justify-content:space-between;align-items:center'>"
                        f"<span style='font-size:.65rem;color:#ffffff;letter-spacing:.07em;text-transform:uppercase'>Matches Hosted</span>"
                        f"<span style='font-size:.9rem;opacity:.35'>🏅</span></div>"
                        f"<div style='font-size:2rem;font-weight:800;color:#00e5d1;line-height:1.2;margin-top:6px'>{mhst}</div>"
                        f"<div style='font-size:.7rem;color:#00e5d1;opacity:.7'>total matches</div>"
                        f"</div>", unsafe_allow_html=True
                    )
                with kc4:
                    st.markdown(
                        f"<div style='{_card_css}'>"
                        f"<div style='display:flex;justify-content:space-between;align-items:center'>"
                        f"<span style='font-size:.65rem;color:#6b7280;letter-spacing:.07em;text-transform:uppercase'>Bat-First Win%</span>"
                        f"<span style='font-size:.9rem;opacity:.35'>🏁</span></div>"
                        f"<div style='font-size:2rem;font-weight:800;color:#4caf50;line-height:1.2;margin-top:6px'>{bf_str}</div>"
                        f"<div style='font-size:.7rem;color:#4caf50;opacity:.85'>{bf_arrow}</div>"
                        f"</div>", unsafe_allow_html=True
                    )
                st.markdown("<div style='margin-bottom:10px'></div>", unsafe_allow_html=True)

                st.markdown(
                    '<div style="margin-top:20px; margin-bottom:16px;">'
                    '<div style="font-size:1.15rem; font-weight:700; color:#ffffff;">📊 Average Runs by Phase at this Venue</div>'
                    '<div style="font-size:0.85rem; color:#9ca3af; margin-top:4px;">Powerplay / Middle / Death — Batting 1st vs 2nd</div>'
                    '</div>',
                    unsafe_allow_html=True
                )

                vdata = df_vph[df_vph["venue"] == sel_venue].copy()

                # Normalise phase names to short labels
                phase_map = {
                    "Powerplay (0-5)"       : "Powerplay (1-6)",
                    "Powerplay (1-6)"       : "Powerplay (1-6)",
                    "Middle Overs (6-14)"   : "Middle (7-15)",
                    "Middle Overs (6-15)"   : "Middle (7-15)",
                    "Death Overs (15-19)"   : "Death (16-20)",
                    "Death Overs (16-20)"   : "Death (16-20)",
                }
                vdata["phase_label"] = vdata["match_phase"].map(
                    lambda x: next((v for k, v in phase_map.items() if k in x), x)
                )
                
                # Merge duplicates after normalisation, preserving inning
                vdata = vdata.groupby(["phase_label", "inning"], as_index=False)["avg_runs"].mean().round(1)

                phase_order  = ["Powerplay (1-6)", "Middle (7-15)", "Death (16-20)"]

                if vdata.empty:
                    st.info(f"No phase data for **{sel_venue}**.")
                else:
                    y_inn1 = []
                    y_inn2 = []
                    for ph in phase_order:
                        r1 = vdata[(vdata["phase_label"] == ph) & (vdata["inning"] == 1)]
                        r2 = vdata[(vdata["phase_label"] == ph) & (vdata["inning"] == 2)]
                        y_inn1.append(float(r1["avg_runs"].iloc[0]) if not r1.empty else 0.0)
                        y_inn2.append(float(r2["avg_runs"].iloc[0]) if not r2.empty else 0.0)

                    t_inn1 = [f"{v}" if v > 0 else "" for v in y_inn1]
                    t_inn2 = [f"{v}" if v > 0 else "" for v in y_inn2]

                    fig_ph = go.Figure()
                    fig_ph.add_trace(go.Bar(
                        name="1st Innings",
                        x=phase_order,
                        y=y_inn1,
                        marker_color="#00e5d1",
                        marker_line_width=0,
                        text=t_inn1,
                        textposition="outside",
                        textfont={"size": 12, "color": "#00e5d1"},
                        hovertemplate="<b>1st Innings</b><br>Phase: %{x}<br>Avg Runs: <b>%{y}</b><extra></extra>"
                    ))
                    fig_ph.add_trace(go.Bar(
                        name="2nd Innings",
                        x=phase_order,
                        y=y_inn2,
                        marker_color="#f5c518",
                        marker_line_width=0,
                        text=t_inn2,
                        textposition="outside",
                        textfont={"size": 12, "color": "#f5c518"},
                        hovertemplate="<b>2nd Innings</b><br>Phase: %{x}<br>Avg Runs: <b>%{y}</b><extra></extra>"
                    ))

                    theme(
                        fig_ph,
                        barmode="group",
                        xaxis_title="Match Phase",
                        yaxis_title="Average Runs",
                        height=400,
                        xaxis={"categoryorder": "array", "categoryarray": phase_order}
                    )
                    # We handle title in HTML/Markdown above, so remove the builtin title string to save space
                    fig_ph.update_layout(title=None, margin={"t": 10})
                    
                    st.plotly_chart(fig_ph, width='stretch')

                # Highest Totals Leaderboard
                st.markdown(
                    f'<div style="margin-top:40px; margin-bottom:16px;">'
                    f'<div style="font-size:1.15rem; font-weight:700; color:#ffffff;">🏆 Highest Totals at {sel_venue}</div>'
                    f'<div style="font-size:0.85rem; color:#ffffff; margin-top:4px;">Top scoring matches at this ground</div>'
                    f'</div>',
                    unsafe_allow_html=True
                )
                df_tops = get_venue_highest_totals(sel_venue)
                if not df_tops.empty:
                    html_table = (
                        '<div style="overflow-x:auto;">\n'
                        '<table style="width:100%; text-align:left; border-collapse:collapse; background:transparent;">\n'
                        '<thead>\n'
                        '<tr>\n'
                        '<th style="padding:10px 10px 10px 0; border-bottom:1px solid #2a3548; color:#00e5d1; font-weight:600; font-size:0.75rem; letter-spacing:0.05em;">DATE</th>\n'
                        '<th style="padding:10px; border-bottom:1px solid #2a3548; color:#00e5d1; font-weight:600; font-size:0.75rem; letter-spacing:0.05em;">TEAMS</th>\n'
                        '<th style="padding:10px; border-bottom:1px solid #2a3548; color:#00e5d1; font-weight:600; font-size:0.75rem; letter-spacing:0.05em;">1ST INNINGS</th>\n'
                        '<th style="padding:10px; border-bottom:1px solid #2a3548; color:#00e5d1; font-weight:600; font-size:0.75rem; letter-spacing:0.05em;">2ND INNINGS</th>\n'
                        '<th style="padding:10px; border-bottom:1px solid #2a3548; color:#00e5d1; font-weight:600; font-size:0.75rem; letter-spacing:0.05em;">WINNER</th>\n'
                        '<th style="padding:10px; border-bottom:1px solid #2a3548; color:#00e5d1; font-weight:600; font-size:0.75rem; letter-spacing:0.05em;">MARGIN</th>\n'
                        '</tr>\n'
                        '</thead>\n'
                        '<tbody>\n'
                    )
                    for _, row in df_tops.iterrows():
                        margin = "-"
                        if pd.notna(row['win_by_runs']) and row['win_by_runs'] > 0:
                            margin = f"{int(row['win_by_runs'])} runs"
                        elif pd.notna(row['win_by_wickets']) and row['win_by_wickets'] > 0:
                            margin = f"{int(row['win_by_wickets'])} wkts"
                        
                        date_str = str(row['date'])[:10] if pd.notna(row['date']) else ""
                        teams_str = row['teams']
                        inn1_str = str(int(row['inn1'])) if pd.notna(row['inn1']) else "-"
                        inn2_str = str(int(row['inn2'])) if pd.notna(row['inn2']) else "-"
                        winner_str = str(row['winner']) if pd.notna(row['winner']) else "-"

                        html_table += (
                            '<tr>\n'
                            f'<td style="padding:12px 10px 12px 0; border-bottom:1px solid #2a3548; color:#ffffff; font-size:0.85rem;">{date_str}</td>\n'
                            f'<td style="padding:12px 10px; border-bottom:1px solid #2a3548; color:#ffffff; font-size:0.85rem;">{teams_str}</td>\n'
                            f'<td style="padding:12px 10px; border-bottom:1px solid #2a3548; color:#00e5d1; font-weight:bold; font-size:0.9rem;">{inn1_str}</td>\n'
                            f'<td style="padding:12px 10px; border-bottom:1px solid #2a3548; color:#f5c518; font-weight:bold; font-size:0.9rem;">{inn2_str}</td>\n'
                            f'<td style="padding:12px 10px; border-bottom:1px solid #2a3548; color:#00e5d1; font-weight:bold; font-size:0.85rem;">{winner_str}</td>\n'
                            f'<td style="padding:12px 10px; border-bottom:1px solid #2a3548; color:#ffffff; font-size:0.85rem;">{margin}</td>\n'
                            '</tr>\n'
                        )
                    html_table += "</tbody></table></div>"
                    st.markdown(html_table, unsafe_allow_html=True)
                else:
                    st.info("No match data available for this venue.")

                # Toss Advantage & Score Distribution
                st.markdown("<div style='margin-bottom:30px'></div>", unsafe_allow_html=True)
                c_ta, c_sd = st.columns(2)
                
                with c_ta:
                    st.markdown(
                        '<div style="margin-bottom:12px;">'
                        '<div style="font-size:1.15rem; font-weight:700; color:#ffffff;">🎯 Toss Advantage</div>'
                        '<div style="font-size:0.85rem; color:#ffffff; margin-top:4px;">Bat first vs bowl first wins at this venue</div>'
                        '</div>',
                        unsafe_allow_html=True
                    )
                    
                    df_wins = get_venue_win_types(sel_venue)
                    if not df_wins.empty:
                        bw = int(df_wins['bat_first'].iloc[0]) if pd.notna(df_wins['bat_first'].iloc[0]) else 0
                        fw = int(df_wins['bowl_first'].iloc[0]) if pd.notna(df_wins['bowl_first'].iloc[0]) else 0
                        
                        if bw == 0 and fw == 0:
                            st.info("No win data available.")
                        else:
                            fig_donut = go.Figure(go.Pie(
                                labels=["Bat First Wins", "Bowl First Wins"],
                                values=[bw, fw],
                                hole=0.6,
                                marker_colors=["#00e5d1", "#f5c518"],
                                textinfo="percent+value",
                                textfont={"color": "#ffffff", "size": 14}
                            ))
                            fig_donut.update_layout(
                                margin={"t": 10, "b": 10, "l": 10, "r": 10},
                                paper_bgcolor="#0d1117",
                                plot_bgcolor="#1a2332",
                                legend={
                                    "orientation": "h",
                                    "yanchor": "top",
                                    "y": -0.1,
                                    "xanchor": "center",
                                    "x": 0.5,
                                    "font": {"color": "#ffffff"}
                                }
                            )
                            st.plotly_chart(fig_donut, width='stretch')
                            
                with c_sd:
                    st.markdown(
                        '<div style="margin-bottom:12px;">'
                        '<div style="font-size:1.15rem; font-weight:700; color:#ffffff;">📦 Score Distribution</div>'
                        '<div style="font-size:0.85rem; color:#ffffff; margin-top:4px;">1st vs 2nd innings score spread at this venue</div>'
                        '</div>',
                        unsafe_allow_html=True
                    )
                    
                    df_scores = get_venue_innings_scores(sel_venue)
                    if not df_scores.empty:
                        fig_vio = go.Figure()
                        
                        scores_1 = df_scores[df_scores['inning'] == 1]['score']
                        scores_2 = df_scores[df_scores['inning'] == 2]['score']
                        
                        if not scores_1.empty:
                            fig_vio.add_trace(go.Violin(
                                y=scores_1,
                                name="1st Innings",
                                box_visible=True,
                                meanline_visible=True,
                                fillcolor="rgba(0, 229, 209, 0.4)",
                                line_color="#00e5d1"
                            ))
                        if not scores_2.empty:
                            fig_vio.add_trace(go.Violin(
                                y=scores_2,
                                name="2nd Innings",
                                box_visible=True,
                                meanline_visible=True,
                                fillcolor="rgba(245, 197, 24, 0.4)",
                                line_color="#f5c518"
                            ))
                            
                        fig_vio.update_layout(
                            margin={"t": 10, "b": 10, "l": 10, "r": 10},
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="#1a2332",
                            yaxis={
                                "title": "Runs",
                                "showgrid": True,
                                "gridcolor": "rgba(255,255,255,0.1)",
                                "zeroline": False
                            },
                            xaxis={"showgrid": False},
                            showlegend=False
                        )
                        st.plotly_chart(fig_vio, width='stretch')
                    else:
                        st.info("No score distribution data available.")

# ══════════════════════════════════════════════════════════════
# TAB 5 — IMPACT LEADERBOARD
# ══════════════════════════════════════════════════════════════
with T[4]:
    st.markdown('<div class="sec">⭐ Player Impact Leaderboard</div>', unsafe_allow_html=True)
    st.markdown("`📐 Impact = (0.5×Runs) + (15×Wickets) + (0.2×SR) − (0.3×Economy)`")

    nm5  = C(df_bat,  "player_name","batter","name")
    rc5  = C(df_bat,  "total_runs","runs")
    src5 = C(df_bat,  "strike_rate")
    tc5  = C(df_bat,  "team")
    bn5  = C(df_bowl, "player_name","bowler","name")
    wk5  = C(df_bowl, "wickets","total_wickets")
    ec5  = C(df_bowl, "economy_rate","economy")

    if nm5 and rc5:
        keep5 = [nm5] + ([tc5] if tc5 else []) + [rc5] + ([src5] if src5 else [])
        imp   = df_bat[keep5].copy()
        if bn5 and wk5:
            bk5 = [bn5] + ([wk5] if wk5 else []) + ([ec5] if ec5 else [])
            imp = imp.merge(df_bowl[bk5].rename(columns={bn5:nm5}), on=nm5, how="left")

        for cn in ["wickets_y","economy_y"]:
            if cn not in imp.columns:
                imp[cn] = 0
        imp = imp.fillna(0)

        awc = wk5 if (wk5 and wk5 in imp.columns) else "wickets_y"
        aec = ec5 if (ec5 and ec5 in imp.columns) else "economy_y"
        asr = src5 if (src5 and src5 in imp.columns) else rc5

        imp["impact"] = (
            0.5 * imp[rc5] + 15 * imp[awc] + 0.2 * imp[asr] - 0.3 * imp[aec]
        ).round(1)

        top25 = imp.nlargest(25, "impact")
        c1, c2 = st.columns([3,2])

        with c1:
            fig = px.bar(top25.head(15), x="impact", y=nm5, orientation="h",
                         color="impact", color_continuous_scale="RdYlGn",
                         title="Top 15 Most Impactful Players",
                         labels={"impact":"Impact Score ⭐", nm5:""})
            theme(fig, coloraxis_showscale=False)
            fig.update_yaxes(categoryorder="total ascending")
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, width='stretch')

        with c2:
            if tc5 and tc5 in top25.columns:
                fig2 = px.treemap(top25, path=[tc5, nm5], values="impact",
                                  color="impact", color_continuous_scale="RdYlGn",
                                  title="Impact by Team (Treemap)")
                theme(fig2, grid=False, coloraxis_showscale=False)
                st.plotly_chart(fig2, width='stretch')
            else:
                fig2 = go.Figure(go.Pie(
                    labels=top25[nm5].head(10),
                    values=top25["impact"].head(10),
                    hole=0.4,
                    marker={"colors": COLORS, "line": {"color": "#0b0d17", "width": 1}}
                ))
                theme(fig2, grid=False, title={"text": "Impact Share Top 10", "font_size": 13})
                st.plotly_chart(fig2, width='stretch')

        dcols5 = [nm5] + ([tc5] if (tc5 and tc5 in top25.columns) else []) + [rc5, "impact"]
        st.markdown('<div class="sec" style="font-size:1rem">Full Leaderboard</div>', unsafe_allow_html=True)
        st.dataframe(
            top25[dcols5].reset_index(drop=True)
                .style
                .background_gradient(subset=["impact"], cmap="RdYlGn")
                .format({rc5:"{:,.0f}", "impact":"{:.1f}"}),
            width='stretch', height=430
        )
    else:
        st.info(f"Batting columns: {df_bat.columns.tolist()}")

# ══════════════════════════════════════════════════════════════
# TAB 6 — PHASE ANALYSIS
# ══════════════════════════════════════════════════════════════
with T[5]:
    st.markdown('<div class="sec" style="font-size:2rem; color:#ffffff; font-weight:900;">📊 Phase Analysis</div>', unsafe_allow_html=True)
    
    if not all_teams:
        st.warning("⚠️ No team data available. Check database connection.")
    else:
        sel_pha_team = st.selectbox("🎯 Select Team for Phase Breakdown", all_teams, index=0, key="pha_team_sel")
        st.markdown('<div style="font-size:0.85rem; color:#ffffff; margin-bottom:20px;">Breakdown by Powerplay (1–6), Middle Overs (7–15), and Death Overs (16–20)</div>', unsafe_allow_html=True)
        
        # Phase KPI Grid
        df_team_pha, pha_err = get_team_phase_stats(sel_pha_team)
        if pha_err:
            st.error(f"❌ Phase data query failed: `{pha_err}`")
        elif df_team_pha.empty:
            st.warning(f"⚠️ No phase data found for **{sel_pha_team}**. The team may not exist in the database or fact_delivery has no records for them.")
        else:
            p_agg = df_team_pha.groupby('phase').agg({
                'match_id': 'nunique',
                'runs': 'sum',
                'wickets': 'sum',
                'valid_balls': 'sum'
            }).reset_index()

            def get_row_stats(ph_key):
                r = p_agg[p_agg['phase'] == ph_key]
                if r.empty: return 0, 0, 0
                m = r['match_id'].iloc[0]
                runs = r['runs'].iloc[0]
                wkts = r['wickets'].iloc[0]
                balls = r['valid_balls'].iloc[0]
                return (runs/m if m else 0, wkts/m if m else 0, (runs/balls*6 if balls else 0))

            rows_data = [
                ("Powerplay (1–6)", "Powerplay", "#00e5d1", "⚡"),
                ("Middle Overs (7–15)", "Middle", "#f5c518", "🛡️"),
                ("Death Overs (16–20)", "Death", "#ff4d4d", "🧨")
            ]

            # ── KPI Cards ──
            for label, key, color, icon in rows_data:
                avg_r, avg_w, rr = get_row_stats(key)
                c1, c2, c3 = st.columns(3)
                card_items = [
                    (f"AVG RUNS · {label.upper()}", f"{avg_r:.1f}", "🏏"),
                    (f"AVG WICKETS · {label.upper()}", f"{avg_w:.1f}", "☝️"),
                    (f"RUN RATE · {label.upper()}", f"{rr:.2f}", icon)
                ]
                for i, (l, v, ico) in enumerate(card_items):
                    with [c1, c2, c3][i]:
                        st.markdown(f"""
                            <div style="background:#1a2332; padding:20px; border-radius:12px; border:1px solid {color}; 
                                        position:relative; margin-bottom:15px; min-height:110px; display:flex; flex-direction:column; justify-content:center;">
                                <div style="font-size:0.65rem; color:#f8fafc; text-transform:uppercase; letter-spacing:1px; font-weight:700;">{l}</div>
                                <div style="position:absolute; top:12px; right:15px; font-size:1.4rem; opacity:0.15;">{ico}</div>
                                <div style="font-size:1.8rem; font-weight:800; color:{color}; margin-top:5px;">{v}</div>
                            </div>
                        """, unsafe_allow_html=True)
            st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)

            # ── Run Rate Progression Chart ──
            st.markdown(
                '<div style="margin-top:20px; margin-bottom:12px;">'
                '<div style="font-size:1.15rem; font-weight:700; color:#ffffff;">📋 Run Rate Progression — Over by Over</div>'
                '<div style="font-size:0.85rem; color:#9ca3af; margin-top:4px;">Cumulative run rate with phase shading</div>'
                '</div>',
                unsafe_allow_html=True
            )

            df_obo = get_team_over_by_over(sel_pha_team)
            if not df_obo.empty:
                # Calculate cumulative runs and cumulative valid balls to get cumulative run rate
                df_obo['cum_runs'] = df_obo['total_runs'].cumsum()
                df_obo['cum_balls'] = df_obo['valid_balls'].cumsum()
                
                # Cumulative Run Rate (runs / valid_balls * 6)
                df_obo['cum_rr'] = (df_obo['cum_runs'] / df_obo['cum_balls'].replace(0,1)) * 6
                
                # Make sure overs go 1 to 20 exactly
                fig_prog = go.Figure()
                
                fig_prog.add_trace(go.Scatter(
                    x=df_obo['over_num'],
                    y=df_obo['cum_rr'],
                    mode='lines+markers',
                    line=dict(color='#00e5d1', width=3, shape='spline', smoothing=1.3),
                    marker=dict(size=6, color='#00e5d1', line=dict(color='#0d1117', width=1)),
                    hovertemplate="<b>Over %{x}</b><br>Run Rate: <b>%{y:.2f}</b><extra></extra>",
                    showlegend=False
                ))

                # Set Y axis range based on data with some padding (around 8 to 12 as requested)
                min_rr = max(0, df_obo['cum_rr'].min() - 0.5)
                max_rr = df_obo['cum_rr'].max() + 0.5
                
                if max_rr < 12: max_rr = 12
                if min_rr > 8: min_rr = max(0, min_rr - 1)

                fig_prog.update_layout(
                    xaxis=dict(
                        title="Over", 
                        tickmode='linear', 
                        tick0=1, 
                        dtick=1, 
                        range=[0.5, 20.5],
                        showgrid=False
                    ),
                    yaxis=dict(
                        title="Run Rate (Cumulative)", 
                        range=[min_rr, max_rr],
                        showgrid=True,
                        gridcolor="rgba(255,255,255,0.05)",
                        zeroline=False
                    ),
                    height=350,
                    margin=dict(t=30, b=40, l=50, r=20),
                    paper_bgcolor="rgba(0,0,0,0)",
                    plot_bgcolor="rgba(0,0,0,0)"
                )
                
                # Add Phase Shading (vrect)
                # Powerplay: Overs 1 to 6
                fig_prog.add_vrect(
                    x0=0.5, x1=6.5,
                    fillcolor="#00e5d1", opacity=0.08,
                    layer="below", line_width=0
                )
                
                # Middle: Overs 6.5 to 15.5
                fig_prog.add_vrect(
                    x0=6.5, x1=15.5,
                    fillcolor="#f5c518", opacity=0.04,
                    layer="below", line_width=0
                )
                
                # Death: Overs 15.5 to 20.5
                fig_prog.add_vrect(
                    x0=15.5, x1=20.5,
                    fillcolor="#ff4d4d", opacity=0.08,
                    layer="below", line_width=0
                )
                
                # Add Phase Labels
                fig_prog.add_annotation(
                    x=2, y=max_rr,
                    text="Powerplay",
                    showarrow=False,
                    font=dict(color="#00e5d1", size=12),
                    yanchor="bottom"
                )
                fig_prog.add_annotation(
                    x=10, y=max_rr,
                    text="Middle",
                    showarrow=False,
                    font=dict(color="#f5c518", size=12),
                    yanchor="bottom"
                )
                fig_prog.add_annotation(
                    x=17, y=max_rr,
                    text="Death",
                    showarrow=False,
                    font=dict(color="#ff4d4d", size=12),
                    yanchor="bottom"
                )
                
                st.plotly_chart(fig_prog, width='stretch')
            else:
                st.info("No over-by-over progression data available.")

            # ── Phase-wise Team Comparison Chart ──
            st.markdown(
                '<div style="margin-top:40px; margin-bottom:12px;">'
                '<div style="font-size:1.15rem; font-weight:700; color:#ffffff;">📊 Phase-wise Team Comparison</div>'
                '<div style="font-size:0.85rem; color:#9ca3af; margin-top:4px;">Select teams to compare their scoring breakdown per phase</div>'
                '</div>',
                unsafe_allow_html=True
            )
            
            # Team Selection for Comparison
            default_comp_teams = ['India', 'Australia', 'England', 'Pakistan', 'South Africa']
            existing_teams = sorted(all_teams) if all_teams else []
            sel_comp_teams = st.multiselect("🎯 Select Teams to Compare", existing_teams, 
                                            default=[t for t in default_comp_teams if t in existing_teams],
                                            key="pha_comp_teams")
            
            if not sel_comp_teams:
                st.info("💡 Please select at least one team to view the comparison.")
            else:
                df_cmp = get_compare_teams_phase_stats()
                if not df_cmp.empty:
                    # Filter only selected teams
                    df_cmp = df_cmp[df_cmp['team'].isin(sel_comp_teams)]
                    
                    # Normalise phase names (consistent with Venue phase chart)
                    pm = {
                        "Powerplay (0-5)": "Powerplay", "Powerplay (1-6)": "Powerplay",
                        "Middle Overs (6-14)": "Middle", "Middle Overs (6-15)": "Middle",
                        "Death Overs (15-19)": "Death", "Death Overs (16-20)": "Death"
                    }
                    df_cmp["phase_key"] = df_cmp["phase"].map(lambda x: next((v for k,v in pm.items() if k in x), x))
                    
                    # Get unique teams from the data
                    teams_list = sorted(df_cmp['team'].unique().tolist())
                    # Reverse for Plotly horizontal bars (renders bottom to top)
                    teams_list_rev = list(reversed(teams_list))
                    
                    fig_cmp = go.Figure()
                    
                    phase_colors = {
                        'Powerplay': '#00e5d1',
                        'Middle': '#f5c518',
                        'Death': '#ff6b6b'
                    }
                    
                    # We need to map phase keys nicely
                    phase_name_map = {
                        'Powerplay': 'Powerplay Overs',
                        'Middle': 'Middle Overs',
                        'Death': 'Death Overs'
                    }
                    
                    for ph in ['Powerplay', 'Middle', 'Death']:
                        ph_data = df_cmp[df_cmp['phase_key'] == ph]
                        
                        x_vals = [] # Teams
                        y_vals = [] # Runs
                        
                        for t in sorted(sel_comp_teams):
                            x_vals.append(t)
                            row = ph_data[ph_data['team'] == t]
                            if not row.empty:
                                y_vals.append(round(row['avg_runs'].iloc[0], 1))
                            else:
                                y_vals.append(0)
                                
                        fig_cmp.add_trace(go.Bar(
                            name=phase_name_map[ph],
                            x=x_vals,
                            y=y_vals,
                            marker_color=phase_colors[ph],
                            marker_line_width=0,
                            text=[f"{v:.1f}" if v > 0 else "" for v in y_vals],
                            textposition="outside",
                            textfont=dict(color="#ffffff", size=10),
                            hovertemplate="<b>%{x} / "+ph+"</b><br>Avg Runs: <b>%{y}</b><extra></extra>"
                        ))
                        
                    fig_cmp.update_layout(
                        barmode="group",
                        xaxis=dict(
                            title="",
                            showgrid=False,
                            tickfont=dict(color="#ffffff", size=11),
                            tickangle=-30
                        ),
                        yaxis=dict(
                            title="Avg Runs Scored",
                            range=[0, 100],
                            showgrid=True,
                            gridcolor="rgba(255,255,255,0.05)",
                            zeroline=False,
                            tickfont=dict(color="#ffffff")
                        ),
                        height=500,
                        margin=dict(t=40, b=60, l=10, r=10),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)",
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.05,
                            xanchor="center",
                            x=0.5,
                            font=dict(color="#ffffff", size=11)
                        )
                    )
                    
                    st.plotly_chart(fig_cmp, width='stretch')
                else:
                    st.info("No phase comparison data available.")



# ── FOOTER ───────────────────────────────────────────────────
st.markdown("""
<div style='border-top:1px solid rgba(0,242,254,.1);padding:16px 0 8px;text-align:center;margin-top:24px'>
  <span style='font-size:.75rem;color:#334155'>
    🏏 T20 WC '26 Master Analytics &nbsp;·&nbsp; PostgreSQL Gold Layer
    &nbsp;·&nbsp; Streamlit + Plotly 6 &nbsp;·&nbsp; © 2026
  </span>
</div>
""", unsafe_allow_html=True)
