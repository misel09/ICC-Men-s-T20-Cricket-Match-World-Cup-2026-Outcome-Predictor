import gradio as gr
import pandas as pd
from catboost import CatBoostClassifier, CatBoostRegressor
import numpy as np
import os

# Model paths
MODEL_DIR = "models"
MATCH_MODEL_PATH = os.path.join(MODEL_DIR, "cricket_match_predictor.cbm")
PLAYING11_MODEL_PATH = os.path.join(MODEL_DIR, "playing11_model (2).cbm")

SCORE_MODELS = {
    "total_runs": os.path.join(MODEL_DIR, "total_runs_model.cbm"),
    "pp_runs": os.path.join(MODEL_DIR, "pp_runs_model.cbm"),
    "mid_runs": os.path.join(MODEL_DIR, "mid_runs_model.cbm"),
    "death_runs": os.path.join(MODEL_DIR, "death_runs_model.cbm"),
    "pp_wickets": os.path.join(MODEL_DIR, "pp_wickets_model.cbm"),
    "mid_wickets": os.path.join(MODEL_DIR, "mid_wickets_model.cbm"),
    "death_wickets": os.path.join(MODEL_DIR, "death_wickets_model.cbm"),
}

# Load models
match_model = None
playing11_model = None
score_models = {}

def load_models():
    global match_model, playing11_model, score_models
    try:
        match_model = CatBoostClassifier()
        match_model.load_model(MATCH_MODEL_PATH)
        print("✅ Loaded match prediction model")
    except Exception as e:
        print(f"❌ Failed to load match model: {e}")

    try:
        playing11_model = CatBoostClassifier()
        playing11_model.load_model(PLAYING11_MODEL_PATH)
        print("✅ Loaded playing11 prediction model")
    except Exception as e:
        print(f"❌ Failed to load playing11 model: {e}")

    for name, path in SCORE_MODELS.items():
        try:
            score_models[name] = CatBoostRegressor()
            score_models[name].load_model(path)
            print(f"✅ Loaded {name} model")
        except Exception as e:
            print(f"❌ Failed to load {name} model: {e}")

load_models()

def predict_match(team1, team2, venue, first_innings_team, is_team1_batting_first,
                 home_advantage, h2h_win_rate, recent_form_t1, recent_form_t2,
                 team1_overall_win_rate, team2_overall_win_rate, avg_score_venue,
                 toss_venue_win_rate, venue_win_rate_t1, venue_win_rate_t2,
                 t1_top_batsmen_avg, t2_top_batsmen_avg, t1_top_bowler_econ, t2_top_bowler_econ):

    if match_model is None:
        return "Model not loaded", 0.0

    features = [team1, team2, venue, first_innings_team, is_team1_batting_first,
               home_advantage, h2h_win_rate, recent_form_t1, recent_form_t2,
               team1_overall_win_rate, team2_overall_win_rate, avg_score_venue,
               toss_venue_win_rate, venue_win_rate_t1, venue_win_rate_t2,
               t1_top_batsmen_avg, t2_top_batsmen_avg, t1_top_bowler_econ, t2_top_bowler_econ]

    columns = ['team1', 'team2', 'venue', 'first_innings_team', 'is_team1_batting_first',
              'home_advantage', 'h2h_win_rate', 'recent_form_t1', 'recent_form_t2',
              'team1_overall_win_rate', 'team2_overall_win_rate', 'avg_score_venue',
              'toss_venue_win_rate', 'venue_win_rate_t1', 'venue_win_rate_t2',
              't1_top_batsmen_avg', 't2_top_batsmen_avg', 't1_top_bowler_econ', 't2_top_bowler_econ']

    df = pd.DataFrame([features], columns=columns)
    pred_proba = match_model.predict_proba(df)[0]
    winner_idx = np.argmax(pred_proba)
    winner = team1 if winner_idx == 0 else team2

    return winner, float(pred_proba[winner_idx])

def predict_score(batting_team, bowling_team, venue, innings, toss_winner, toss_decision,
                 venue_avg_runs, venue_avg_pp_runs, venue_avg_mid_runs, venue_avg_death_runs,
                 venue_avg_pp_wickets, venue_avg_mid_wickets, venue_avg_death_wickets,
                 team_avg_runs, team_avg_pp_runs, team_avg_mid_runs, team_avg_death_runs,
                 opponent_avg_runs_conceded, opponent_avg_pp_runs_conceded, opponent_avg_mid_runs_conceded, opponent_avg_death_runs_conceded,
                 team_last5_avg_runs, opponent_last5_avg_conceded, team_win_rate_vs_opponent,
                 attack_vs_defense, venue_vs_team, recent_vs_average):

    if not score_models:
        return "Models not loaded"

    features = [batting_team, bowling_team, venue, innings, toss_winner, toss_decision,
               venue_avg_runs, venue_avg_pp_runs, venue_avg_mid_runs, venue_avg_death_runs,
               venue_avg_pp_wickets, venue_avg_mid_wickets, venue_avg_death_wickets,
               team_avg_runs, team_avg_pp_runs, team_avg_mid_runs, team_avg_death_runs,
               opponent_avg_runs_conceded, opponent_avg_pp_runs_conceded, opponent_avg_mid_runs_conceded, opponent_avg_death_runs_conceded,
               team_last5_avg_runs, opponent_last5_avg_conceded, team_win_rate_vs_opponent,
               attack_vs_defense, venue_vs_team, recent_vs_average]

    columns = ['batting_team', 'bowling_team', 'venue', 'innings', 'toss_winner', 'toss_decision',
              'venue_avg_runs', 'venue_avg_pp_runs', 'venue_avg_mid_runs', 'venue_avg_death_runs',
              'venue_avg_pp_wickets', 'venue_avg_mid_wickets', 'venue_avg_death_wickets',
              'team_avg_runs', 'team_avg_pp_runs', 'team_avg_mid_runs', 'team_avg_death_runs',
              'opponent_avg_runs_conceded', 'opponent_avg_pp_runs_conceded', 'opponent_avg_mid_runs_conceded', 'opponent_avg_death_runs_conceded',
              'team_last5_avg_runs', 'opponent_last5_avg_conceded', 'team_win_rate_vs_opponent',
              'attack_vs_defense', 'venue_vs_team', 'recent_vs_average']

    df = pd.DataFrame([features], columns=columns)

    predictions = {}
    for name, model in score_models.items():
        pred = model.predict(df)[0]
        predictions[name] = float(pred)

    result = f"""
Total Runs: {predictions['total_runs']:.1f}
Powerplay Runs: {predictions['pp_runs']:.1f}
Middle Overs Runs: {predictions['mid_runs']:.1f}
Death Overs Runs: {predictions['death_runs']:.1f}
Powerplay Wickets: {predictions['pp_wickets']:.1f}
Middle Overs Wickets: {predictions['mid_wickets']:.1f}
Death Overs Wickets: {predictions['death_wickets']:.1f}
"""
    return result

# Gradio Interface
with gr.Blocks(title="Cricket Match Prediction") as demo:
    gr.Markdown("# 🏏 Cricket Match Prediction System")

    with gr.Tab("Match Winner Prediction"):
        gr.Markdown("Predict the winner of a cricket match")

        with gr.Row():
            team1 = gr.Textbox(label="Team 1", placeholder="e.g., India")
            team2 = gr.Textbox(label="Team 2", placeholder="e.g., Australia")

        with gr.Row():
            venue = gr.Textbox(label="Venue", placeholder="e.g., Melbourne Cricket Ground")
            first_innings_team = gr.Textbox(label="First Innings Team", placeholder="e.g., India")

        with gr.Row():
            is_team1_batting_first = gr.Number(label="Is Team 1 Batting First (0/1)", value=1)
            home_advantage = gr.Number(label="Home Advantage", value=0.0)

        with gr.Row():
            h2h_win_rate = gr.Number(label="Head-to-Head Win Rate", value=0.5)
            recent_form_t1 = gr.Number(label="Team 1 Recent Form", value=0.5)
            recent_form_t2 = gr.Number(label="Team 2 Recent Form", value=0.5)

        with gr.Row():
            team1_overall_win_rate = gr.Number(label="Team 1 Overall Win Rate", value=0.6)
            team2_overall_win_rate = gr.Number(label="Team 2 Overall Win Rate", value=0.4)

        with gr.Row():
            avg_score_venue = gr.Number(label="Average Score at Venue", value=150.0)
            toss_venue_win_rate = gr.Number(label="Toss Venue Win Rate", value=0.5)

        with gr.Row():
            venue_win_rate_t1 = gr.Number(label="Venue Win Rate Team 1", value=0.5)
            venue_win_rate_t2 = gr.Number(label="Venue Win Rate Team 2", value=0.5)

        with gr.Row():
            t1_top_batsmen_avg = gr.Number(label="Team 1 Top Batsmen Average", value=35.0)
            t2_top_batsmen_avg = gr.Number(label="Team 2 Top Batsmen Average", value=30.0)

        with gr.Row():
            t1_top_bowler_econ = gr.Number(label="Team 1 Top Bowler Economy", value=7.0)
            t2_top_bowler_econ = gr.Number(label="Team 2 Top Bowler Economy", value=8.0)

        match_predict_btn = gr.Button("Predict Match Winner")
        match_output = gr.Textbox(label="Prediction Result")

        match_predict_btn.click(
            predict_match,
            inputs=[team1, team2, venue, first_innings_team, is_team1_batting_first,
                   home_advantage, h2h_win_rate, recent_form_t1, recent_form_t2,
                   team1_overall_win_rate, team2_overall_win_rate, avg_score_venue,
                   toss_venue_win_rate, venue_win_rate_t1, venue_win_rate_t2,
                   t1_top_batsmen_avg, t2_top_batsmen_avg, t1_top_bowler_econ, t2_top_bowler_econ],
            outputs=match_output
        )

    with gr.Tab("Score Prediction"):
        gr.Markdown("Predict scores and wickets for different phases of the innings")

        with gr.Row():
            batting_team = gr.Textbox(label="Batting Team", placeholder="e.g., India")
            bowling_team = gr.Textbox(label="Bowling Team", placeholder="e.g., Australia")
            venue = gr.Textbox(label="Venue", placeholder="e.g., Melbourne Cricket Ground")

        with gr.Row():
            innings = gr.Number(label="Innings", value=1)
            toss_winner = gr.Textbox(label="Toss Winner", placeholder="e.g., India")
            toss_decision = gr.Textbox(label="Toss Decision", placeholder="bat/field")

        with gr.Row():
            venue_avg_runs = gr.Number(label="Venue Average Runs", value=150.0)
            venue_avg_pp_runs = gr.Number(label="Venue PP Average Runs", value=45.0)
            venue_avg_mid_runs = gr.Number(label="Venue Mid Average Runs", value=70.0)
            venue_avg_death_runs = gr.Number(label="Venue Death Average Runs", value=35.0)

        with gr.Row():
            venue_avg_pp_wickets = gr.Number(label="Venue PP Average Wickets", value=1.5)
            venue_avg_mid_wickets = gr.Number(label="Venue Mid Average Wickets", value=3.0)
            venue_avg_death_wickets = gr.Number(label="Venue Death Average Wickets", value=2.5)

        with gr.Row():
            team_avg_runs = gr.Number(label="Team Average Runs", value=160.0)
            team_avg_pp_runs = gr.Number(label="Team PP Average Runs", value=50.0)
            team_avg_mid_runs = gr.Number(label="Team Mid Average Runs", value=75.0)
            team_avg_death_runs = gr.Number(label="Team Death Average Runs", value=35.0)

        with gr.Row():
            opponent_avg_runs_conceded = gr.Number(label="Opponent Average Runs Conceded", value=140.0)
            opponent_avg_pp_runs_conceded = gr.Number(label="Opponent PP Runs Conceded", value=40.0)
            opponent_avg_mid_runs_conceded = gr.Number(label="Opponent Mid Runs Conceded", value=65.0)
            opponent_avg_death_runs_conceded = gr.Number(label="Opponent Death Runs Conceded", value=35.0)

        with gr.Row():
            team_last5_avg_runs = gr.Number(label="Team Last 5 Matches Average Runs", value=155.0)
            opponent_last5_avg_conceded = gr.Number(label="Opponent Last 5 Matches Average Conceded", value=145.0)
            team_win_rate_vs_opponent = gr.Number(label="Team Win Rate vs Opponent", value=0.6)

        with gr.Row():
            attack_vs_defense = gr.Number(label="Attack vs Defense", value=1.0)
            venue_vs_team = gr.Number(label="Venue vs Team", value=1.0)
            recent_vs_average = gr.Number(label="Recent vs Average", value=1.0)

        score_predict_btn = gr.Button("Predict Scores")
        score_output = gr.Textbox(label="Prediction Result", lines=8)

        score_predict_btn.click(
            predict_score,
            inputs=[batting_team, bowling_team, venue, innings, toss_winner, toss_decision,
                   venue_avg_runs, venue_avg_pp_runs, venue_avg_mid_runs, venue_avg_death_runs,
                   venue_avg_pp_wickets, venue_avg_mid_wickets, venue_avg_death_wickets,
                   team_avg_runs, team_avg_pp_runs, team_avg_mid_runs, team_avg_death_runs,
                   opponent_avg_runs_conceded, opponent_avg_pp_runs_conceded, opponent_avg_mid_runs_conceded, opponent_avg_death_runs_conceded,
                   team_last5_avg_runs, opponent_last5_avg_conceded, team_win_rate_vs_opponent,
                   attack_vs_defense, venue_vs_team, recent_vs_average],
            outputs=score_output
        )

if __name__ == "__main__":
    demo.launch()