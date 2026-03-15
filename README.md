---
title: Cricket Match Prediction System
emoji: 🏏
colorFrom: blue
colorTo: green
sdk: gradio
sdk_version: 4.0.0
app_file: gradio_app.py
pinned: false
---

# Cricket Match Prediction System

An interactive web application for predicting cricket match outcomes, playing 11 selections, and score predictions using machine learning models.

## Features

- **Match Winner Prediction**: Predict which team will win based on various match features
- **Score Prediction**: Predict runs and wickets for different phases of the innings (Powerplay, Middle, Death overs)

## How to Use

1. **Match Prediction Tab**:
   - Enter team names, venue, and various statistical features
   - Click "Predict Match Winner" to get the predicted winner and probability

2. **Score Prediction Tab**:
   - Enter batting/bowling teams, venue, and performance statistics
   - Click "Predict Scores" to get predictions for different innings phases

## Models

The application uses pre-trained CatBoost models for predictions:
- Match outcome prediction
- Score prediction across different overs phases

## Data Requirements

The predictions require various cricket statistics as input. In a real application, these would be fetched from a database or API, but here they're entered manually for demonstration.
```bash
./run_pipeline.bat
```

This processes cricket data and updates the database for model training.