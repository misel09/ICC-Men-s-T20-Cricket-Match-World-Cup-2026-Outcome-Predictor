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
        predictions[name] = int(round(float(pred)))

    result = f"""
Total Runs: {predictions['total_runs']}
Powerplay Runs: {predictions['pp_runs']}
Middle Overs Runs: {predictions['mid_runs']}
Death Overs Runs: {predictions['death_runs']}
Powerplay Wickets: {predictions['pp_wickets']}
Middle Overs Wickets: {predictions['mid_wickets']}
Death Overs Wickets: {predictions['death_wickets']}
"""
    return result

from feature_extractor import extractor

# Gradio Interface
with gr.Blocks(title="Cricket Match Prediction") as demo:
    gr.Markdown("# 🏏 Cricket Match Prediction System")

    with gr.Tab("Match Winner Prediction"):
        gr.Markdown("Predict the winner of a cricket match by simply entering the teams and venue.")

        with gr.Row():
            team1 = gr.Textbox(label="Team 1", placeholder="e.g., India")
            team2 = gr.Textbox(label="Team 2", placeholder="e.g., Australia")

        with gr.Row():
            venue = gr.Textbox(label="Venue", placeholder="e.g., Melbourne Cricket Ground")

        match_predict_btn = gr.Button("Predict Match Winner")
        match_output = gr.Textbox(label="Prediction Result")

        def simplified_predict_match(t1, t2, ven):
            if not t1 or not t2 or not ven:
                return "Please enter Team 1, Team 2, and Venue."
                
            features = extractor.get_match_features(t1, t2, ven)
            EXPECTED_FEATURES = [
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
            
            df = pd.DataFrame([features], columns=EXPECTED_FEATURES)
            pred_proba = match_model.predict_proba(df)[0]
            winner_idx = np.argmax(pred_proba)
            winner = t1 if winner_idx == 0 else t2
            return f"{winner} (Confidence: {pred_proba[winner_idx]*100:.1f}%)"

        match_predict_btn.click(
            simplified_predict_match,
            inputs=[team1, team2, venue],
            outputs=match_output
        )

    with gr.Tab("Playing 11 Prediction"):
        gr.Markdown("Predict the most likely Playing XI for a team in a specific match setup.")

        with gr.Row():
            p11_team = gr.Textbox(label="Team", placeholder="e.g., India")
            p11_opponent = gr.Textbox(label="Opponent", placeholder="e.g., Australia")
            p11_venue = gr.Textbox(label="Venue", placeholder="e.g., Melbourne Cricket Ground")

        p11_predict_btn = gr.Button("Predict Playing 11")
        p11_output = gr.Dataframe(headers=["Player Name", "Role", "Probability"], interactive=False)

        def predict_playing_11(team, opponent, venue):
            if not team or not opponent or not venue:
                return pd.DataFrame([["Please enter Team, Opponent, and Venue", "", ""]])
                
            if playing11_model is None:
                return pd.DataFrame([["Model not loaded", "", ""]])
                
            team_players, team_map, role_map = extractor.get_playing11_features(team, opponent, venue)
            if team_players.empty:
                return pd.DataFrame([[f"No data found for team {team} or unable to encode inputs.", "", ""]])

            features = [
                'team','opponent','venue','player_role','designation',
                'batting_average', 'strike_rate', 'bowling_economy',
                'career_total_runs', 'career_wickets',
                'runs_last_5_matches', 'wickets_last_5_matches',
                'runs_at_venue', 'wickets_at_venue',
                'runs_vs_opponent', 'batting_avg_vs_opponent', 'wickets_vs_opponent',
                'bowling_econ_vs_opponent', 'wickets_last5_vs_opponent',
                'selection_rate'
            ]
            
            # Predict Probability
            team_players['prob'] = playing11_model.predict_proba(team_players[features])[:,1]
            
            # Aggregate Player Scores
            players = team_players.groupby(
                ['player_name','team','player_role']
            )['prob'].mean().reset_index()

            # Map categories back
            players['player_role'] = players['player_role'].map(role_map)
            
            # Select Top 11
            playing_xi = players.sort_values('prob', ascending=False).head(11)
            playing_xi['prob'] = (playing_xi['prob'] * 100).round(1).astype(str) + "%"
            
            return playing_xi[['player_name', 'player_role', 'prob']]

        p11_predict_btn.click(
            predict_playing_11,
            inputs=[p11_team, p11_opponent, p11_venue],
            outputs=p11_output
        )

    with gr.Tab("Score Prediction"):
        gr.Markdown("Predict scores and wickets by specifying the batting/bowling teams and venue.")

        with gr.Row():
            batting_team = gr.Textbox(label="Batting Team", placeholder="e.g., India")
            bowling_team = gr.Textbox(label="Bowling Team", placeholder="e.g., Australia")
            venue_score = gr.Textbox(label="Venue", placeholder="e.g., Melbourne Cricket Ground")

        score_predict_btn = gr.Button("Predict Scores")
        score_output = gr.Textbox(label="Prediction Result", lines=8)

        def simplified_predict_score(bat, bowl, ven):
            if not bat or not bowl or not ven:
                return "Please enter Batting Team, Bowling Team, and Venue."
                
            features = extractor.get_score_features(bat, bowl, ven)
            args = [
                bat, bowl, ven,
                features['innings'],
                features['toss_winner'],
                features['toss_decision'],
                features['venue_avg_runs'],
                features['venue_avg_pp_runs'],
                features['venue_avg_mid_runs'],
                features['venue_avg_death_runs'],
                features['venue_avg_pp_wickets'],
                features['venue_avg_mid_wickets'],
                features['venue_avg_death_wickets'],
                features['team_avg_runs'],
                features['team_avg_pp_runs'],
                features['team_avg_mid_runs'],
                features['team_avg_death_runs'],
                features['opponent_avg_runs_conceded'],
                features['opponent_avg_pp_runs_conceded'],
                features['opponent_avg_mid_runs_conceded'],
                features['opponent_avg_death_runs_conceded'],
                features['team_last5_avg_runs'],
                features['opponent_last5_avg_conceded'],
                features['team_win_rate_vs_opponent'],
                features['attack_vs_defense'],
                features['venue_vs_team'],
                features['recent_vs_average']
            ]
            
            return predict_score(*args)

        score_predict_btn.click(
            simplified_predict_score,
            inputs=[batting_team, bowling_team, venue_score],
            outputs=score_output
        )

if __name__ == "__main__":
    demo.launch()