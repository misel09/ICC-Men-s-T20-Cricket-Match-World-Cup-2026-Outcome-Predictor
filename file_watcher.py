"""
File Watcher – Auto-ETL on new JSON files
==========================================
Monitors your DATA_DIR folder in real time.
When a new .json match file is detected, it immediately:
  1. Runs etl.py        → loads new match into Postgres
  2. Runs match_names.py → links player names
  3. Runs build_features.py + build_playing11_features.py

Usage:
    py file_watcher.py

Keep this running in the background. Safe to restart anytime.
"""

import os
import sys
import time
import subprocess
import logging
from pathlib import Path
from dotenv import load_dotenv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ── Config ────────────────────────────────────────────────────────────────────
load_dotenv()

PROJECT_DIR = Path(__file__).parent
DATA_DIR    = os.getenv("DATA_DIR", str(PROJECT_DIR / "data"))
PYTHON      = sys.executable   # uses the same Python that launched this script

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_DIR / "watcher.log", encoding="utf-8"),
    ]
)
log = logging.getLogger(__name__)


# ── Pipeline runner ───────────────────────────────────────────────────────────
def run_pipeline():
    """Run the full ETL pipeline. ETL is idempotent — safe to re-run."""
    steps = [
        ("ETL (JSON → Postgres)",         "etl.py"),
        ("Match player names",             "match_names.py"),
        ("Build match features",           "build_features.py"),
        ("Build playing-11 features",      "build_playing11_features.py"),
    ]
    for label, script in steps:
        log.info(f"  ▶ Running: {label}")
        result = subprocess.run(
            [PYTHON, str(PROJECT_DIR / script)],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            log.info(f"  ✓ Done: {label}")
        else:
            log.error(f"  ✗ Failed: {label}\n{result.stderr}")
            break   # stop pipeline on failure


# ── Watchdog event handler ────────────────────────────────────────────────────
class NewJsonHandler(FileSystemEventHandler):
    """Triggered whenever a file is created or moved into the watched folder."""

    def __init__(self):
        self._cooldown = {}   # path → last trigger time (debounce)

    def on_created(self, event):
        self._handle(event)

    def on_moved(self, event):
        # Treat a file moved into the folder the same as created
        event.src_path = event.dest_path
        self._handle(event)

    def _handle(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix.lower() != ".json":
            return

        # Debounce: ignore duplicate events within 5 seconds for same file
        now = time.time()
        last = self._cooldown.get(str(path), 0)
        if now - last < 5:
            return
        self._cooldown[str(path)] = now

        log.info(f"\n{'='*55}")
        log.info(f"🆕 New JSON detected: {path.name}")
        log.info(f"{'='*55}")
        run_pipeline()
        log.info("✅ Pipeline complete. Watching for more files...\n")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info(f"👀 Watching folder: {DATA_DIR}")
    log.info("Drop a new .json file there and it will auto-load into Postgres!")
    log.info("Press Ctrl+C to stop.\n")

    handler  = NewJsonHandler()
    observer = Observer()
    observer.schedule(handler, path=DATA_DIR, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping watcher...")
        observer.stop()
    observer.join()
    log.info("Watcher stopped.")
