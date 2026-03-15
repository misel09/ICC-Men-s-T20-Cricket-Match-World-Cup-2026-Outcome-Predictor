#!/usr/bin/env python3
"""
Test script for Cricket Prediction API
"""

import requests
import json

BASE_URL = "http://localhost:8000"

def test_health():
    """Test health check endpoint"""
    try:
        response = requests.get(f"{BASE_URL}/")
        print(f"Health Check: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

def test_match_prediction():
    """Test match prediction endpoint"""
    data = {
        "team1": "India",
        "team2": "Australia",
        "venue": "Melbourne Cricket Ground",
        "first_innings_team": "India",
        "is_team1_batting_first": 1,
        "home_advantage": 0.5,
        "h2h_win_rate": 0.5,
        "recent_form_t1": 0.6,
        "recent_form_t2": 0.7,
        "team1_overall_win_rate": 0.55,
        "team2_overall_win_rate": 0.6,
        "avg_score_venue": 160.0,
        "toss_venue_win_rate": 0.52,
        "venue_win_rate_t1": 0.53,
        "venue_win_rate_t2": 0.58,
        "t1_top_batsmen_avg": 35.2,
        "t2_top_batsmen_avg": 32.8,
        "t1_top_bowler_econ": 7.2,
        "t2_top_bowler_econ": 7.5
    }

    try:
        response = requests.post(f"{BASE_URL}/predict_match", json=data)
        print(f"Match Prediction: {response.status_code}")
        if response.status_code == 200:
            print(f"Response: {response.json()}")
        else:
            print(f"Error: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"Match prediction failed: {e}")
        return False

def test_score_prediction():
    """Test score prediction endpoint"""
    data = {
        "batting_team": "India",
        "bowling_team": "Australia",
        "venue": "Melbourne Cricket Ground",
        "innings": 1,
        "toss_winner": "India",
        "toss_decision": "bat",
        "venue_avg_runs": 160.0,
        "venue_avg_pp_runs": 45.0,
        "venue_avg_mid_runs": 70.0,
        "venue_avg_death_runs": 45.0,
        "venue_avg_pp_wickets": 1.2,
        "venue_avg_mid_wickets": 2.5,
        "venue_avg_death_wickets": 2.8,
        "team_avg_runs": 155.0,
        "team_avg_pp_runs": 44.0,
        "team_avg_mid_runs": 68.0,
        "team_avg_death_runs": 43.0,
        "opponent_avg_runs_conceded": 158.0,
        "opponent_avg_pp_runs_conceded": 46.0,
        "opponent_avg_mid_runs_conceded": 72.0,
        "opponent_avg_death_runs_conceded": 40.0,
        "team_last5_avg_runs": 155.0,
        "opponent_last5_avg_conceded": 158.0,
        "team_win_rate_vs_opponent": 0.5,
        "attack_vs_defense": 0.98,
        "venue_vs_team": 0.97,
        "recent_vs_average": 1.0
    }

    try:
        response = requests.post(f"{BASE_URL}/predict_score", json=data)
        print(f"Score Prediction: {response.status_code}")
        if response.status_code == 200:
            print(f"Response: {response.json()}")
        else:
            print(f"Error: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"Score prediction failed: {e}")
        return False

def test_playing11():
    """Test playing11 prediction endpoint"""
    data = {
        "players": [
            {"name": "Virat Kohli", "role": "batsman"},
            {"name": "Jasprit Bumrah", "role": "bowler"},
            {"name": "Rohit Sharma", "role": "batsman"}
        ]
    }

    try:
        response = requests.post(f"{BASE_URL}/predict_playing11", json=data)
        print(f"Playing11 Prediction: {response.status_code}")
        if response.status_code == 200:
            print(f"Response: {response.json()}")
        else:
            print(f"Error: {response.text}")
        return response.status_code == 200
    except Exception as e:
        print(f"Playing11 prediction failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing Cricket Prediction API")
    print("=" * 40)

    # Test all endpoints
    health_ok = test_health()
    print()

    if health_ok:
        match_ok = test_match_prediction()
        print()
        score_ok = test_score_prediction()
        print()
        playing11_ok = test_playing11()
        print()

        print("📊 Test Results:")
        print(f"Health Check: {'✅' if health_ok else '❌'}")
        print(f"Match Prediction: {'✅' if match_ok else '❌'}")
        print(f"Score Prediction: {'✅' if score_ok else '❌'}")
        print(f"Playing11 Prediction: {'✅' if playing11_ok else '❌'}")
    else:
        print("❌ API not accessible, stopping tests")