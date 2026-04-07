"""
scheduler.py
------------
Automates the full pipeline: Data Refresh → ETL → Forecast
Runs on a configurable schedule (default: daily at 06:00).

Usage:
    python scheduler/scheduler.py              # runs on schedule (daemon mode)
    python scheduler/scheduler.py --run-now    # single immediate run
"""

import sys
import os
import argparse
import logging
from datetime import datetime

# Add src/ to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import schedule
import time
from etl_pipeline import run_etl
from forecasting import run_forecast

BASE        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORTS_DIR = os.path.join(BASE, 'reports')
os.makedirs(REPORTS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(REPORTS_DIR, 'scheduler_log.txt')),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)

def full_pipeline():
    """Run ETL + Forecast end-to-end."""
    start = datetime.now()
    log.info(f"\n{'#'*60}")
    log.info(f"SCHEDULED PIPELINE RUN  {start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'#'*60}")

    try:
        log.info("Step 1/2 — Running ETL pipeline...")
        run_etl()
        log.info("Step 2/2 — Running forecasting...")
        run_forecast(horizon=6)
        elapsed = (datetime.now() - start).seconds
        log.info(f"\nPipeline completed in {elapsed}s  ✓")
    except Exception as e:
        log.error(f"Pipeline FAILED: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description='Sales Pipeline Scheduler')
    parser.add_argument('--run-now',  action='store_true',
                        help='Run pipeline immediately then exit')
    parser.add_argument('--time',     default='06:00',
                        help='Daily run time HH:MM (default: 06:00)')
    parser.add_argument('--interval', type=int, default=None,
                        help='Run every N minutes instead of daily schedule')
    args = parser.parse_args()

    if args.run_now:
        log.info("--run-now flag detected. Running pipeline immediately.")
        full_pipeline()
        return

    if args.interval:
        log.info(f"Scheduler started — running every {args.interval} minute(s)")
        schedule.every(args.interval).minutes.do(full_pipeline)
    else:
        log.info(f"Scheduler started — running daily at {args.time}")
        schedule.every().day.at(args.time).do(full_pipeline)

    log.info("Waiting for next scheduled run... (Ctrl+C to stop)\n")
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == '__main__':
    main()
