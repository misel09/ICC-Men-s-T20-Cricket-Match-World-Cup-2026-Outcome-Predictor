import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import os
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

/* ── Main background: deep dark navy matching reference image ── */
[data-testid="stAppViewContainer"]{
  background:#0d1117 !important;
}
[data-testid="stAppViewBlock"]{
  background:#0d1117 !important;
}
/* Cover the default white block */
.main .block-container{
  background:#0d1117 !important;
  padding-top:1.5rem;
}
section[data-testid="stSidebar"]{
  background:#0b0e18 !important;
  border-right:1px solid rgba(0,229,229,.10);
}
[data-testid="stSidebar"] *{color:#ffffff!important;}

/* Tab bar */
.stTabs [data-baseweb="tab-list"]{
  background:#111827;border-radius:12px;padding:4px 6px;gap:4px;
  border:1px solid rgba(0,229,229,.1);
}
.stTabs [data-baseweb="tab"]{
  border-radius:9px;padding:4px 12px;color:#ffffff;font-weight:700;font-size:2rem;
  transition:all .2s;
}
.stTabs [aria-selected="true"]{
  background:linear-gradient(135deg,rgba(0,229,229,.15),rgba(56,189,248,.08));
  color:#00e5e5!important;border:1px solid rgba(0,229,229,.35);
  box-shadow:0 0 16px rgba(0,229,229,.12);
}

/* KPI Cards — dark slate like the reference */
.kcard{
  background:#111827;
  border:1px solid rgba(0,229,229,.18);
  border-radius:14px;padding:20px 16px;
  text-align:center;position:relative;overflow:hidden;
}
.kcard::before{
  content:'';position:absolute;top:0;left:0;right:0;height:2px;
  background:linear-gradient(90deg,transparent,#00e5e5,transparent);
}
.kval{font-size:2rem;font-weight:900;color:#f59e0b;line-height:1.1;}
.klbl{font-size:.67rem;color:#ffffff;text-transform:uppercase;
      letter-spacing:1.8px;margin-top:5px;}
.kico{font-size:1.2rem;margin-bottom:5px;}

/* Section headings */
.sec{font-size:1.2rem;font-weight:800;
  background:linear-gradient(90deg,#00e5e5,#38bdf8);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  margin:8px 0 14px 0;letter-spacing:-.2px;}

h1,h2,h3{color:#00e5e5!important;}
p,label,.stMarkdown p{color:#ffffff;}

/* Inputs */
.stSelectbox>div,.stMultiSelect>div{
  background:#111827!important;border-color:rgba(0,229,229,.2)!important;
  border-radius:9px!important;color:#e2e8f0!important;
}

/* Data table */
div[data-testid="stDataFrame"]{border-radius:10px;overflow:hidden;}

/* Scrollbar */
::-webkit-scrollbar{width:5px;}
::-webkit-scrollbar-track{background:#0d1117;}
::-webkit-scrollbar-thumb{background:rgba(0,229,229,.25);border-radius:3px;}
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
    try:
        host = os.getenv("DB_HOST", "").strip()
        port = os.getenv("DB_PORT", "").strip()
        user = os.getenv("DB_USER", "").strip()
        password = os.getenv("DB_PASSWORD", "").strip()
        dbname = os.getenv("DB_NAME", "").strip()
        
        if not all([host, port, user, dbname]):
            return None
            
        return psycopg2.connect(
            host     = host,
            port     = int(port),
            user     = user,
            password = password,
            dbname   = dbname
        )
    except Exception:
        return None

@st.cache_data(ttl=3600, show_spinner=False)
def qry(view):
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(f"SELECT * FROM {view}", conn)
        for c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="ignore")
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
    conn = get_conn()
    if conn is None:
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
        df = pd.read_sql(sql, conn, params={'ta': ta, 'tb': tb})
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
    conn = get_conn()
    if conn is None:
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
        df = pd.read_sql(sql, conn, params={'ta': ta, 'tb': tb})
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_phase_runs():
    """Avg runs per match phase per venue from vw_phase_analysis, split by inning."""
    conn = get_conn()
    if conn is None:
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
        return pd.read_sql(sql, conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_custom_kpis(venue: str):
    """Direct queries for KPIs to handle venues filtered out of vw_venue_stats."""
    conn = get_conn()
    if conn is None:
        return "N/A", "N/A", "N/A", None
    try:
        avg1 = avg2 = mhst = "N/A"
        bf_pct = None
        
        # 1. Matches Hosted
        m_df = pd.read_sql("SELECT COUNT(*) AS c FROM dim_match WHERE venue = %(v)s", conn, params={'v': venue})
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
        bf_df = pd.read_sql(sql_bf, conn, params={'v': venue})
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
        inn_df = pd.read_sql(sql_inn, conn, params={'v': venue})
        for _, r in inn_df.iterrows():
            if r['inning'] == 1: avg1 = float(r['avg_score'])
            if r['inning'] == 2: avg2 = float(r['avg_score'])
            
        return avg1, avg2, mhst, bf_pct
    except Exception:
        return "N/A", "N/A", "N/A", None

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_highest_totals(venue: str):
    """Query top matches by 1st innings score at a venue."""
    conn = get_conn()
    if conn is None:
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
        return pd.read_sql(sql, conn, params={'v': venue})
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_win_types(venue: str):
    conn = get_conn()
    if conn is None: return pd.DataFrame()
    try:
        sql = """
            SELECT 
                SUM(CASE WHEN win_by_runs > 0 THEN 1 ELSE 0 END) as bat_first,
                SUM(CASE WHEN win_by_wickets > 0 THEN 1 ELSE 0 END) as bowl_first
            FROM dim_match
            WHERE venue = %(v)s
        """
        return pd.read_sql(sql, conn, params={'v': venue})
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_venue_innings_scores(venue: str):
    conn = get_conn()
    if conn is None: return pd.DataFrame()
    try:
        sql = """
            SELECT f.inning, sum(f.runs_total) as score
            FROM fact_delivery f
            JOIN dim_match m ON f.match_id = m.match_id
            WHERE m.venue = %(v)s AND f.inning IN (1,2)
            GROUP BY m.match_id, f.inning
        """
        return pd.read_sql(sql, conn, params={'v': venue})
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_team_phase_stats(team_name):
    conn = get_conn()
    if conn is None: return pd.DataFrame()
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
        return pd.read_sql(sql, conn, params={'t': team_name})
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_all_phase_stats():
    """Aggregated stats for all teams across phases."""
    conn = get_conn()
    if conn is None: return pd.DataFrame()
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
        return pd.read_sql(sql, conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600, show_spinner=False)
def get_team_over_progression(team_name):
    """Average runs per over (1-20) for the selected team."""
    conn = get_conn()
    if conn is None: return pd.DataFrame()
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
        return pd.read_sql(sql, conn, params={'t': team_name})
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
st.sidebar.markdown("""
<div style='text-align:center;padding:8px 0 20px'>
  <div style='font-size:2.5rem'>🏏</div>
  <div style='font-size:1.1rem;font-weight:800;color:#00f2fe;letter-spacing:1px'>T20 WC '26</div>
  <div style='font-size:.7rem;color:#475569;letter-spacing:2px'>MASTER ANALYTICS</div>
</div>
""", unsafe_allow_html=True)

sc = "#10b981" if db_ok else "#ef4444"
st.sidebar.markdown(
    f"<div style='text-align:center;font-size:.72rem;font-weight:700;color:{sc};"
    f"letter-spacing:1.5px;margin-bottom:12px'>{'🟢 DATABASE LIVE' if db_ok else '🔴 DB OFFLINE'}</div>",
    unsafe_allow_html=True)

all_teams  = sorted(df_bat[tbc].dropna().unique())  if tbc else []
all_venues = sorted(df_ven[vc].dropna().unique())   if vc  else []
sel_teams  = st.sidebar.multiselect("🎯 Filter Teams",  all_teams,  placeholder="All Teams")
sel_venues = st.sidebar.multiselect("🏟️ Filter Venues", all_venues, placeholder="All Venues")

st.sidebar.markdown("---")
pn = C(df_bat, "player_name","batter","name")
bn = C(df_bowl,"player_name","bowler","name")
st.sidebar.markdown("<div style='font-size:.65rem;color:#334155;text-align:center;margin-top:12px'>Refreshes every 60 min · cricket_userQL Gold Layer</div>", unsafe_allow_html=True)

# ── FILTERED FRAMES ──────────────────────────────────────────
bf  = df_bat[df_bat[tbc].isin(sel_teams)]  if (sel_teams and tbc) else df_bat.copy()
bwf = df_bowl[df_bowl[tbcw].isin(sel_teams)] if (sel_teams and tbcw) else df_bowl.copy()
vf  = df_ven[df_ven[vc].isin(sel_venues)]  if (sel_venues and vc) else df_ven.copy()

# ── HEADER ───────────────────────────────────────────────────
st.markdown("""
<div style='padding:6px 0 4px'>
  <h1 style='font-size:2.3rem;font-weight:900;margin:0;
   background:linear-gradient(90deg,#00f2fe,#4facfe,#f59e0b);
   -webkit-background-clip:text;-webkit-text-fill-color:transparent'>
    🏏 T20 WC '26 Master Analytics
  </h1>
  <p style='color:#ffffff;font-size:1.1rem;margin:4px 0 0 2px;font-weight:500'>
    Live Intelligence · cricket_userQL Gold Layer · ICC Men's T20 World Cup 2026
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
                        st.plotly_chart(fig, use_container_width=True)

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
                            st.plotly_chart(fig2, use_container_width=True)
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
                        st.plotly_chart(fig_yr, use_container_width=True)

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
    st.markdown('<div class="sec">Batting Arsenal</div>', unsafe_allow_html=True)
    if bf.empty:
        st.warning("⚠️ No batting data.")
    else:
        nm2  = C(bf, "player_name","batter","name")
        rc2  = C(bf, "total_runs","runs")
        src2 = C(bf, "strike_rate")
        tc2  = C(bf, "team")
        avc2 = C(bf, "average","batting_average")
        fc2  = C(bf, "fours")
        sc62 = C(bf, "sixes")

        topN = st.slider("Top N Players", 10, 30, 15, key="batn")

        if nm2 and rc2:
            top = bf.nlargest(topN, rc2)
            fig = px.bar(top, x=rc2, y=nm2, orientation="h",
                         color=rc2, color_continuous_scale="Tealrose",
                         title=f"Top {topN} Run Scorers",
                         labels={rc2:"Runs", nm2:""})
            theme(fig, coloraxis_showscale=False)
            fig.update_yaxes(categoryorder="total ascending")
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(fig, use_container_width=True)

            # Boundary stacked bar
            if fc2 and sc62 and nm2 and rc2:
                st.markdown('<div class="sec" style="font-size:1rem">Boundary Breakdown</div>', unsafe_allow_html=True)
                t15 = bf.nlargest(15, rc2).sort_values(rc2)
                fig3 = go.Figure([
                    go.Bar(name="Fours", x=t15[nm2], y=t15[fc2],
                           marker_color="#00f2fe", marker_line_width=0),
                    go.Bar(name="Sixes", x=t15[nm2], y=t15[sc62],
                           marker_color="#f59e0b", marker_line_width=0),
                ])
                theme(fig3, barmode="stack", title="Fours vs Sixes (Top 15)")
                fig3.update_xaxes(tickangle=-35)
                st.plotly_chart(fig3, use_container_width=True)

            # Quadrant
            if src2 and avc2:
                st.markdown('<div class="sec" style="font-size:1rem">Efficiency Quadrant</div>', unsafe_allow_html=True)
                qd = bf.dropna(subset=[src2, avc2])
                fig4 = px.scatter(qd, x=avc2, y=src2, hover_name=nm2,
                                  color=tc2 if tc2 else None,
                                  color_discrete_sequence=COLORS,
                                  labels={avc2:"Batting Average", src2:"Strike Rate"},
                                  title="Consistency vs Aggression Quadrant")
                fig4.add_hline(y=float(qd[src2].median()), line_dash="dash",
                               line_color="rgba(255,255,255,.15)")
                fig4.add_vline(x=float(qd[avc2].median()), line_dash="dash",
                               line_color="rgba(255,255,255,.15)")
                fig4.add_annotation(x=qd[avc2].max(), y=qd[src2].max(),
                                    text="🚀 Elite Zone", showarrow=False,
                                    font=dict(color="#10b981", size=11))
                theme(fig4)
                st.plotly_chart(fig4, use_container_width=True)

        # Run distribution
        st.markdown('<div class="sec" style="font-size:1rem">Run Distribution</div>', unsafe_allow_html=True)
        if rc2:
            sd = bf[rc2].dropna()
            fig_h = px.histogram(sd, nbins=30, title="Player Run Totals Distribution",
                                 color_discrete_sequence=["#00f2fe"],
                                 labels={"value":"Runs","count":"Players"})
            fig_h.add_vline(x=float(sd.mean()), line_dash="dash", line_color="#f59e0b",
                            annotation_text=f"Mean: {sd.mean():.0f}",
                            annotation_font_color="#f59e0b")
            fig_h.add_vline(x=float(sd.median()), line_dash="dot", line_color="#10b981",
                            annotation_text=f"Median: {sd.median():.0f}",
                            annotation_font_color="#10b981")
            theme(fig_h)
            st.plotly_chart(fig_h, use_container_width=True)

# ══════════════════════════════════════════════════════════════
# TAB 3 — BOWLING
# ══════════════════════════════════════════════════════════════
with T[2]:
    st.markdown('<div class="sec">Bowling Command Center</div>', unsafe_allow_html=True)
    if bwf.empty:
        st.warning("⚠️ No bowling data.")
    else:
        bnm3 = C(bwf, "player_name","bowler","name")
        wkc3 = C(bwf, "wickets","total_wickets")
        ecc3 = C(bwf, "economy_rate","economy")
        btc3 = C(bwf, "team")
        bsr3 = C(bwf, "bowling_strike_rate","strike_rate")

        topBN = st.slider("Top N Bowlers", 10, 30, 15, key="bowln")

        if bnm3 and wkc3:
            tw3 = bwf.nlargest(topBN, wkc3)
            c1, c2 = st.columns(2)

            with c1:
                fig = px.bar(tw3, x=wkc3, y=bnm3, orientation="h",
                             color=wkc3, color_continuous_scale="RdYlGn_r",
                             title=f"Top {topBN} Wicket Takers",
                             labels={wkc3:"Wickets", bnm3:""})
                theme(fig, coloraxis_showscale=False)
                fig.update_yaxes(categoryorder="total ascending")
                fig.update_traces(marker_line_width=0)
                st.plotly_chart(fig, use_container_width=True)

            with c2:
                if ecc3:
                    bd3 = bwf[bwf[wkc3]>=2].dropna(subset=[ecc3, wkc3])
                    fig2 = px.scatter(bd3, x=ecc3, y=wkc3,
                                      color=btc3 if btc3 else None,
                                      size=wkc3, hover_name=bnm3, size_max=28,
                                      color_discrete_sequence=COLORS,
                                      title="Economy vs Wickets",
                                      labels={ecc3:"Economy Rate", wkc3:"Wickets"})
                    fig2.add_vline(x=float(bd3[ecc3].mean()), line_dash="dash",
                                   line_color="rgba(255,255,255,.2)")
                    theme(fig2)
                    st.plotly_chart(fig2, use_container_width=True)

            if btc3 and ecc3:
                top_t = bwf[btc3].value_counts().nlargest(8).index
                bdf3  = bwf[bwf[btc3].isin(top_t) & (bwf[wkc3]>=2)]
                if not bdf3.empty:
                    fig3 = px.box(bdf3, x=btc3, y=ecc3, color=btc3,
                                  color_discrete_sequence=COLORS,
                                  title="Economy Rate by Team", points="all")
                    theme(fig3)
                    fig3.update_xaxes(tickangle=-30)
                    st.plotly_chart(fig3, use_container_width=True)

            if btc3 and bsr3 and ecc3:
                st.markdown('<div class="sec" style="font-size:1rem">Bowling Efficiency Matrix</div>', unsafe_allow_html=True)
                bm = bwf.dropna(subset=[bsr3, ecc3])
                fig4 = px.scatter(bm, x=ecc3, y=bsr3,
                                  size=wkc3 if wkc3 else None,
                                  color=btc3 if btc3 else None,
                                  hover_name=bnm3,
                                  color_discrete_sequence=COLORS,
                                  labels={ecc3:"Economy Rate", bsr3:"Bowling SR"},
                                  title="SR vs Economy (Bubble)")
                theme(fig4)
                st.plotly_chart(fig4, use_container_width=True)

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
                        xaxis=dict(categoryorder="array", categoryarray=phase_order)
                    )
                    # We handle title in HTML/Markdown above, so remove the builtin title string to save space
                    fig_ph.update_layout(title=None, margin=dict(t=10))
                    
                    st.plotly_chart(fig_ph, use_container_width=True)

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
                                textfont=dict(color="#ffffff", size=14)
                            ))
                            fig_donut.update_layout(
                                margin=dict(t=10, b=10, l=10, r=10),
                                paper_bgcolor="#0d1117",
                                plot_bgcolor="#1a2332",
                                legend=dict(
                                    orientation="h",
                                    yanchor="top",
                                    y=-0.1,
                                    xanchor="center",
                                    x=0.5,
                                    font=dict(color="#ffffff")
                                )
                            )
                            st.plotly_chart(fig_donut, use_container_width=True)
                            
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
                            margin=dict(t=10, b=10, l=10, r=10),
                            paper_bgcolor="rgba(0,0,0,0)",
                            plot_bgcolor="#1a2332",
                            yaxis=dict(
                                title="Runs",
                                showgrid=True,
                                gridcolor="rgba(255,255,255,0.1)",
                                zeroline=False
                            ),
                            xaxis=dict(showgrid=False),
                            showlegend=False
                        )
                        st.plotly_chart(fig_vio, use_container_width=True)
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
            st.plotly_chart(fig, use_container_width=True)

        with c2:
            if tc5 and tc5 in top25.columns:
                fig2 = px.treemap(top25, path=[tc5, nm5], values="impact",
                                  color="impact", color_continuous_scale="RdYlGn",
                                  title="Impact by Team (Treemap)")
                theme(fig2, grid=False, coloraxis_showscale=False)
                st.plotly_chart(fig2, use_container_width=True)
            else:
                fig2 = go.Figure(go.Pie(
                    labels=top25[nm5].head(10),
                    values=top25["impact"].head(10),
                    hole=0.4,
                    marker=dict(colors=COLORS, line=dict(color="#0b0d17", width=1))
                ))
                theme(fig2, grid=False, title=dict(text="Impact Share Top 10", font_size=13))
                st.plotly_chart(fig2, use_container_width=True)

        dcols5 = [nm5] + ([tc5] if (tc5 and tc5 in top25.columns) else []) + [rc5, "impact"]
        st.markdown('<div class="sec" style="font-size:1rem">Full Leaderboard</div>', unsafe_allow_html=True)
        st.dataframe(
            top25[dcols5].reset_index(drop=True)
                .style
                .background_gradient(subset=["impact"], cmap="RdYlGn")
                .format({rc5:"{:,.0f}", "impact":"{:.1f}"}),
            use_container_width=True, height=430
        )
    else:
        st.info(f"Batting columns: {df_bat.columns.tolist()}")

# ══════════════════════════════════════════════════════════════
# TAB 6 — PHASE ANALYSIS
# ══════════════════════════════════════════════════════════════
with T[5]:
    st.markdown('<div class="sec" style="font-size:2rem; color:#ffffff; font-weight:900;">📊 Phase Analysis</div>', unsafe_allow_html=True)
    
    if all_teams:
        sel_pha_team = st.selectbox("🎯 Select Team for Phase Breakdown", all_teams, index=0, key="pha_team_sel")
        st.markdown('<div style="font-size:0.85rem; color:#ffffff; margin-bottom:20px;">Breakdown by Powerplay (1–6), Middle Overs (7–15), and Death Overs (16–20)</div>', unsafe_allow_html=True)
        
        # Phase KPI Grid
        df_team_pha = get_team_phase_stats(sel_pha_team)
        if not df_team_pha.empty:
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

            # --- 📈 Run Rate Progression Section ---
            st.markdown(f"""
                <div style="margin-top:20px; margin-bottom:16px;">
                    <div style="font-size:1.5rem; font-weight:700; color:#ffffff;">📈 Run Rate Progression — Over by Over</div>
                    <div style="font-size:0.85rem; color:#9ca3af; margin-top:4px;">Average runs scored per over for {sel_pha_team}</div>
                </div>
            """, unsafe_allow_html=True)
            
            df_prog = get_team_over_progression(sel_pha_team)
            if not df_prog.empty:
                fig_prog = px.line(df_prog, x="over", y="avg_runs",
                                   labels={"over": "Over", "avg_runs": "Avg Runs"},
                                   markers=True)
                fig_prog.update_traces(line_color="#00f2fe", line_width=3, 
                                     marker=dict(size=8, color="#0b0d17", 
                                     line=dict(width=2, color="#00f2fe")))
                theme(fig_prog, xaxis={"dtick": 1, "range": [0.5, 20.5]}, 
                      yaxis={"gridcolor": "rgba(255,255,255,0.05)"})
                st.plotly_chart(fig_prog, use_container_width=True)
            else:
                st.info("No over-by-over data available.")

            st.markdown("<div style='height:40px'></div>", unsafe_allow_html=True)

            # --- Phase-wise Team Comparison Section ---
            st.markdown("""
                <div style="margin-top:20px; margin-bottom:16px;">
                    <div style="font-size:1.5rem; font-weight:700; color:#ffffff;">📊 Phase-wise Team Comparison</div>
                    <div style="font-size:0.85rem; color:#9ca3af; margin-top:4px;">Runs per phase across all teams</div>
                </div>
            """, unsafe_allow_html=True)

            df_all_pha = get_all_phase_stats()
            if not df_all_pha.empty:
                # Normalise phase names (consistent with Venue phase chart)
                pm = {
                    "Powerplay (0-5)": "Powerplay (1-6)", "Powerplay (1-6)": "Powerplay (1-6)",
                    "Middle Overs (6-14)": "Middle (7-15)", "Middle Overs (6-15)": "Middle (7-15)",
                    "Death Overs (15-19)": "Death (16-20)", "Death Overs (16-20)": "Death (16-20)"
                }
                df_all_pha["phase_label"] = df_all_pha["match_phase"].map(lambda x: next((v for k,v in pm.items() if k in x), x))
                
                # Filter strictly to requested teams & aggregate
                target_teams = ['Afghanistan', 'Australia', 'Canada', 'England', 'India', 'Ireland', 'Italy', 'Namibia', 'Nepal', 'Netherlands']
                df_c = df_all_pha[df_all_pha['team'].isin(target_teams)].copy()
                
                if not df_c.empty:
                    # Pivot to get phases as columns
                    piv = df_c.groupby(['team', 'phase_label'])['avg_runs'].mean().unstack().reset_index()
                    piv = piv.fillna(0)
                    
                    # Ensure requested teams are all present (even with 0)
                    for t in target_teams:
                        if t not in piv['team'].values:
                            piv = pd.concat([piv, pd.DataFrame([{'team': t}])], ignore_index=True)
                    piv = piv[piv['team'].isin(target_teams)].sort_values('team').fillna(0)

                    fig_comp = go.Figure()
                    
                    # Exact specs: orientation="h", specific hex colors
                    fig_comp.add_trace(go.Bar(
                        name="Powerplay", y=piv['team'], x=piv.get('Powerplay (1-6)', [0]*len(piv)),
                        orientation='h', marker_color="#00e5d1"
                    ))
                    fig_comp.add_trace(go.Bar(
                        name="Middle Overs", y=piv['team'], x=piv.get('Middle (7-15)', [0]*len(piv)),
                        orientation='h', marker_color="#f5c518"
                    ))
                    fig_comp.add_trace(go.Bar(
                        name="Death Overs", y=piv['team'], x=piv.get('Death (16-20)', [0]*len(piv)),
                        orientation='h', marker_color="#ff6b6b"
                    ))

                    theme(fig_comp, 
                          barmode='group',
                          height=600,
                          xaxis={"title": "Avg Runs", "range": [0, 90], "gridcolor": "rgba(255,255,255,0.05)"},
                          yaxis={"title": "", "autorange": "reversed"},
                          margin={"l": 10, "r": 10, "t": 10, "b": 10})
                    
                    st.plotly_chart(fig_comp, use_container_width=True)
                else:
                    st.info("No data found for the selected team list.")
            else:
                st.info("Phase comparison data unavailable.")



# ── FOOTER ───────────────────────────────────────────────────
st.markdown("""
<div style='border-top:1px solid rgba(0,242,254,.1);padding:16px 0 8px;text-align:center;margin-top:24px'>
  <span style='font-size:.75rem;color:#334155'>
    🏏 T20 WC '26 Master Analytics &nbsp;·&nbsp; PostgreSQL Gold Layer
    &nbsp;·&nbsp; Streamlit + Plotly 6 &nbsp;·&nbsp; © 2026
  </span>
</div>
""", unsafe_allow_html=True)
