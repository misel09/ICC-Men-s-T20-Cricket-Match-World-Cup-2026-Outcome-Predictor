# 🏏 ICC Men's T20 World Cup Winning Prediction

A comprehensive machine learning platform for predicting ICC Men's T20 World Cup outcomes, including match winners, optimal playing 11, and phase-specific score progression (runs and wickets).

## 🚀 Overview

This project implements an end-to-end data pipeline from raw match data (JSON) to high-performance predictive models. It features multiple interfaces including a FastAPI backend, a Gradio web UI for "what-if" analysis, and a Streamlit dashboard for data exploration.

## ✨ Key Features

- **🏆 Match Winner Prediction**: Predict the probability of a team winning based on historical performance, head-to-head stats, and venue data.
- **🏏 Phase-Specific Score Forecasting**: Predict runs and wickets across different innings phases:
  - **Powerplay** (0-6 overs)
  - **Middle** (6-15 overs) 
  - **Death** (15-20 overs)
- **📋 Playing 11 Optimization**: Recommends the strongest lineup for a given match-up.
- **🔄 Automated ETL Pipeline**: Watchdog-based ingestion that automatically processes new match data from Cricsheet.
- **📊 Interactive Dashboards**: Real-time visualization of team and player statistics.

## 🛠️ Tech Stack

- **ML Framework**: [CatBoost](https://catboost.ai/) (Gradient Boosting)
- **APIs & UI**: FastAPI, Gradio, Streamlit
- **Data Engine**: PostgreSQL, SQLAlchemy, Pandas, NumPy
- **Utilities**: RapidFuzz (Name matching), Watchdog (File monitoring), Docker

## 📁 Project Structure

```text
├── models/                     # Pre-trained .cbm model binaries
├── backend/                    # Streamlit dashboards and DB inspection
├── data/                       # Raw Cricsheet JSON match data
├── etl.py                      # Main ETL logic (JSON -> PostgreSQL)
├── match_names.py              # Fuzzy matching for player/team names
├── build_features.py           # Feature engineering for match prediction
├── build_score_context.py      # Feature engineering for score prediction
├── app.py                      # FastAPI backend entry point
├── gradio_app.py               # Gradio web interface
└── run_pipeline.bat            # Windows automation script
```

## 🚀 Getting Started

### 1. Installation
```bash
pip install -r requirements.txt
```

### 2. Environment Setup
Create a `.env` file in the root directory with your PostgreSQL credentials:
```env
DB_USER=your_user
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cricket_db
```

### 3. Running the Pipeline
To ingest data and update features:
```bash
./run_pipeline.bat
```

### 4. Launching the Apps
- **Gradio UI**: `python gradio_app.py`
- **FastAPI Backend**: `python app.py`
- **Streamlit Dashboard**: `streamlit run backend/streamlit_app.py`

## 🧠 Model Architecture

The system uses a suite of specialized CatBoost models:
- `cricket_match_predictor.cbm`: Core classification model.
- `pp_runs_model.cbm` / `pp_wickets_model.cbm`: Powerplay dynamics.
- `mid_runs_model.cbm` / `mid_wickets_model.cbm`: Middle-overs strategy.
- `death_runs_model.cbm` / `death_wickets_model.cbm`: Death-overs finishing.

---
*Developed for the ICC Men's T20 World Cup Prediction Challenge.*
