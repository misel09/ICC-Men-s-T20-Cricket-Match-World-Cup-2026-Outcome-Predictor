@echo off
:: ─────────────────────────────────────────────────────────────────────────────
:: ICC T20 Cricket Pipeline – Automated Runner
:: Runs every 2 days via Windows Task Scheduler
:: Safe to re-run: ETL skips already-loaded matches (no duplicate data)
:: ─────────────────────────────────────────────────────────────────────────────

SET PROJECT_DIR=d:\Languages\projects\icc_mens_t20_wc_winnig_prediction
SET LOG_FILE=%PROJECT_DIR%\pipeline_run.log

echo. >> %LOG_FILE%
echo ========================================== >> %LOG_FILE%
echo Pipeline started at %DATE% %TIME% >> %LOG_FILE%
echo ========================================== >> %LOG_FILE%

cd /d %PROJECT_DIR%

echo [1/4] Running ETL (JSON to Postgres)... >> %LOG_FILE%
py etl.py >> %LOG_FILE% 2>&1

echo [2/4] Matching player names... >> %LOG_FILE%
py match_names.py >> %LOG_FILE% 2>&1

echo Pipeline finished at %DATE% %TIME% >> %LOG_FILE%
echo ========================================== >> %LOG_FILE%
