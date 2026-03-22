"""
Cricket Prediction API
======================
Hosts the trained CatBoost models for cricket predictions.

Endpoints:
- /predict_match: Predict match winner using cricket_match_predictor.cbm
- /predict_playing11: Predict playing 11 using playing11_model (2).cbm
- /predict_score: Predict scores using the score prediction models
"""

import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from catboost import CatBoostClassifier, CatBoostRegressor
import numpy as np
from typing import List, Dict, Any
from feature_extractor import extractor

app = FastAPI(title="Cricket Prediction API", version="1.0.0")

# Model paths
MODEL_DIR = "models"
MATCH_MODEL_PATH = os.path.join(MODEL_DIR, "cricket_match_predictor.cbm")
PLAYING11_MODEL_PATH = os.path.join(MODEL_DIR, "playing11_model (2).cbm")

# Score prediction models
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
            if "wickets" in name:
                score_models[name] = CatBoostRegressor()
            else:
                score_models[name] = CatBoostRegressor()
            score_models[name].load_model(path)
            print(f"✅ Loaded {name} model")
        except Exception as e:
            print(f"❌ Failed to load {name} model: {e}")

# Load models on startup
load_models()

# Pydantic models for request/response
class MatchPredictionRequest(BaseModel):
    team1: str
    team2: str
    venue: str
    first_innings_team: str
    is_team1_batting_first: int
    home_advantage: float
    h2h_win_rate: float
    recent_form_t1: float
    recent_form_t2: float
    team1_overall_win_rate: float
    team2_overall_win_rate: float
    avg_score_venue: float
    toss_venue_win_rate: float
    venue_win_rate_t1: float
    venue_win_rate_t2: float
    t1_top_batsmen_avg: float
    t2_top_batsmen_avg: float
    t1_top_bowler_econ: float
    t2_top_bowler_econ: float

class MatchPredictionResponse(BaseModel):
    predicted_winner: str
    probability: float

class PlayerFeatures(BaseModel):
    player_name: str
    player_role: str
    designation: str
    batting_average: float
    strike_rate: float
    bowling_economy: float
    career_total_runs: int
    career_wickets: int
    runs_last_5_matches: int
    wickets_last_5_matches: int
    runs_at_venue: int
    wickets_at_venue: int
    runs_vs_opponent: int
    batting_avg_vs_opponent: float
    wickets_vs_opponent: int
    bowling_econ_vs_opponent: float
    wickets_last5_vs_opponent: int
    selection_rate: float

class Playing11Request(BaseModel):
    team: str
    opponent: str
    venue: str

class Playing11Player(BaseModel):
    name: str
    role: str
    probability: str

class Playing11Response(BaseModel):
    team: str
    selected_players: List[Playing11Player]

class ScorePredictionRequest(BaseModel):
    batting_team: str
    bowling_team: str
    venue: str
    innings: int
    toss_winner: str
    toss_decision: str
    venue_avg_runs: float
    venue_avg_pp_runs: float
    venue_avg_mid_runs: float
    venue_avg_death_runs: float
    venue_avg_pp_wickets: float
    venue_avg_mid_wickets: float
    venue_avg_death_wickets: float
    team_avg_runs: float
    team_avg_pp_runs: float
    team_avg_mid_runs: float
    team_avg_death_runs: float
    opponent_avg_runs_conceded: float
    opponent_avg_pp_runs_conceded: float
    opponent_avg_mid_runs_conceded: float
    opponent_avg_death_runs_conceded: float
    team_last5_avg_runs: float
    opponent_last5_avg_conceded: float
    team_win_rate_vs_opponent: float
    attack_vs_defense: float
    venue_vs_team: float
    recent_vs_average: float

class ScorePredictionResponse(BaseModel):
    total_runs: int
    pp_runs: int
    mid_runs: int
    death_runs: int
    pp_wickets: int
    mid_wickets: int
    death_wickets: int

class SimplePredictionRequest(BaseModel):
    team1: str
    team2: str
    venue: str

class SimpleScoreRequest(BaseModel):
    batting_team: str
    bowling_team: str
    venue: str


@app.get("/")
def root():
    return {"message": "Cricket Prediction API", "status": "running"}

@app.post("/predict_match", response_model=MatchPredictionResponse)
def predict_match(request: MatchPredictionRequest):
    if match_model is None:
        raise HTTPException(status_code=503, detail="Match prediction model not loaded")

    # Prepare features in the correct order as per training data
    features = [
        request.team1,
        request.team2,
        request.venue,
        request.first_innings_team,
        request.is_team1_batting_first,
        request.home_advantage,
        request.h2h_win_rate,
        request.recent_form_t1,
        request.recent_form_t2,
        request.team1_overall_win_rate,
        request.team2_overall_win_rate,
        request.avg_score_venue,
        request.toss_venue_win_rate,
        request.venue_win_rate_t1,
        request.venue_win_rate_t2,
        request.t1_top_batsmen_avg,
        request.t2_top_batsmen_avg,
        request.t1_top_bowler_econ,
        request.t2_top_bowler_econ
    ]

    # Create DataFrame with proper column names
    columns = [
        'team1', 'team2', 'venue', 'first_innings_team', 'is_team1_batting_first',
        'home_advantage', 'h2h_win_rate', 'recent_form_t1', 'recent_form_t2',
        'team1_overall_win_rate', 'team2_overall_win_rate', 'avg_score_venue',
        'toss_venue_win_rate', 'venue_win_rate_t1', 'venue_win_rate_t2',
        't1_top_batsmen_avg', 't2_top_batsmen_avg', 't1_top_bowler_econ', 't2_top_bowler_econ'
    ]

    df = pd.DataFrame([features], columns=columns)

    # Make prediction
    pred_proba = match_model.predict_proba(df)[0]
    winner_idx = np.argmax(pred_proba)
    winner = request.team1 if winner_idx == 0 else request.team2

    return MatchPredictionResponse(
        predicted_winner=winner,
        probability=float(pred_proba[winner_idx])
    )

@app.post("/predict_playing11", response_model=Playing11Response)
def predict_playing11(request: Playing11Request):
    if playing11_model is None:
        raise HTTPException(status_code=503, detail="Playing11 prediction model not loaded")

    team_players, team_map, role_map = extractor.get_playing11_features(request.team, request.opponent, request.venue)
    if team_players.empty:
        raise HTTPException(status_code=404, detail=f"No data found for team {request.team} or invalid encoding.")

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

    players['player_role'] = players['player_role'].map(role_map)
    
    playing_xi = players.sort_values('prob', ascending=False).head(11)
    
    selected = [
        Playing11Player(
            name=row['player_name'],
            role=row['player_role'],
            probability=f"{(row['prob'] * 100):.1f}%"
        )
        for _, row in playing_xi.iterrows()
    ]

    return Playing11Response(team=request.team, selected_players=selected)

@app.post("/predict_score", response_model=ScorePredictionResponse)
def predict_score(request: ScorePredictionRequest):
    if not score_models:
        raise HTTPException(status_code=503, detail="Score prediction models not loaded")

    # Prepare features for all score models
    features = [
        request.batting_team,
        request.bowling_team,
        request.venue,
        request.innings,
        request.toss_winner,
        request.toss_decision,
        request.venue_avg_runs,
        request.venue_avg_pp_runs,
        request.venue_avg_mid_runs,
        request.venue_avg_death_runs,
        request.venue_avg_pp_wickets,
        request.venue_avg_mid_wickets,
        request.venue_avg_death_wickets,
        request.team_avg_runs,
        request.team_avg_pp_runs,
        request.team_avg_mid_runs,
        request.team_avg_death_runs,
        request.opponent_avg_runs_conceded,
        request.opponent_avg_pp_runs_conceded,
        request.opponent_avg_mid_runs_conceded,
        request.opponent_avg_death_runs_conceded,
        request.team_last5_avg_runs,
        request.opponent_last5_avg_conceded,
        request.team_win_rate_vs_opponent,
        request.attack_vs_defense,
        request.venue_vs_team,
        request.recent_vs_average
    ]

    columns = [
        'batting_team', 'bowling_team', 'venue', 'innings', 'toss_winner', 'toss_decision',
        'venue_avg_runs', 'venue_avg_pp_runs', 'venue_avg_mid_runs', 'venue_avg_death_runs',
        'venue_avg_pp_wickets', 'venue_avg_mid_wickets', 'venue_avg_death_wickets',
        'team_avg_runs', 'team_avg_pp_runs', 'team_avg_mid_runs', 'team_avg_death_runs',
        'opponent_avg_runs_conceded', 'opponent_avg_pp_runs_conceded', 'opponent_avg_mid_runs_conceded', 'opponent_avg_death_runs_conceded',
        'team_last5_avg_runs', 'opponent_last5_avg_conceded', 'team_win_rate_vs_opponent',
        'attack_vs_defense', 'venue_vs_team', 'recent_vs_average'
    ]

    df = pd.DataFrame([features], columns=columns)

    predictions = {}
    for name, model in score_models.items():
        pred = model.predict(df)[0]
        predictions[name] = int(round(float(pred)))

    return ScorePredictionResponse(**predictions)

@app.post("/predict_match_simple", response_model=MatchPredictionResponse)
def predict_match_simple(request: SimplePredictionRequest):
    if match_model is None:
        raise HTTPException(status_code=503, detail="Match prediction model not loaded")

    features_dict = extractor.get_match_features(request.team1, request.team2, request.venue)
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
    
    df = pd.DataFrame([features_dict], columns=EXPECTED_FEATURES)
    pred_proba = match_model.predict_proba(df)[0]
    winner_idx = np.argmax(pred_proba)
    winner = request.team1 if winner_idx == 0 else request.team2

    return MatchPredictionResponse(predicted_winner=winner, probability=float(pred_proba[winner_idx]))

@app.post("/predict_score_simple", response_model=ScorePredictionResponse)
def predict_score_simple(request: SimpleScoreRequest):
    if not score_models:
        raise HTTPException(status_code=503, detail="Score prediction models not loaded")

    feat = extractor.get_score_features(request.batting_team, request.bowling_team, request.venue)
    
    features = [
        request.batting_team, request.bowling_team, request.venue,
        feat['innings'], feat['toss_winner'], feat['toss_decision'],
        feat['venue_avg_runs'], feat['venue_avg_pp_runs'], feat['venue_avg_mid_runs'], feat['venue_avg_death_runs'],
        feat['venue_avg_pp_wickets'], feat['venue_avg_mid_wickets'], feat['venue_avg_death_wickets'],
        feat['team_avg_runs'], feat['team_avg_pp_runs'], feat['team_avg_mid_runs'], feat['team_avg_death_runs'],
        feat['opponent_avg_runs_conceded'], feat['opponent_avg_pp_runs_conceded'], feat['opponent_avg_mid_runs_conceded'], feat['opponent_avg_death_runs_conceded'],
        feat['team_last5_avg_runs'], feat['opponent_last5_avg_conceded'], feat['team_win_rate_vs_opponent'],
        feat['attack_vs_defense'], feat['venue_vs_team'], feat['recent_vs_average']
    ]

    columns = [
        'batting_team', 'bowling_team', 'venue', 'innings', 'toss_winner', 'toss_decision',
        'venue_avg_runs', 'venue_avg_pp_runs', 'venue_avg_mid_runs', 'venue_avg_death_runs',
        'venue_avg_pp_wickets', 'venue_avg_mid_wickets', 'venue_avg_death_wickets',
        'team_avg_runs', 'team_avg_pp_runs', 'team_avg_mid_runs', 'team_avg_death_runs',
        'opponent_avg_runs_conceded', 'opponent_avg_pp_runs_conceded', 'opponent_avg_mid_runs_conceded', 'opponent_avg_death_runs_conceded',
        'team_last5_avg_runs', 'opponent_last5_avg_conceded', 'team_win_rate_vs_opponent',
        'attack_vs_defense', 'venue_vs_team', 'recent_vs_average'
    ]

    df = pd.DataFrame([features], columns=columns)
    predictions = {name: int(round(float(model.predict(df)[0]))) for name, model in score_models.items()}

    return ScorePredictionResponse(**predictions)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)