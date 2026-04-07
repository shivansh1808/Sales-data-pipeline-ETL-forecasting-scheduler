"""
main.py
-------
Single entry point. Runs the full pipeline:
  1. Generate raw data (skip if already exists)
  2. ETL — clean, transform, load to DB + processed CSVs
  3. Forecast — 6-month revenue forecast
  4. Print summary report

Usage:
    python main.py               # full run
    python main.py --regenerate  # force regenerate raw data
"""

import os
import sys
import argparse
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(levelname)s  %(message)s')
log = logging.getLogger(__name__)

BASE    = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE, 'data', 'raw')

def main():
    parser = argparse.ArgumentParser(description='Sales Dashboard Pipeline')
    parser.add_argument('--regenerate', action='store_true',
                        help='Force regenerate raw data even if it exists')
    parser.add_argument('--forecast-only', action='store_true',
                        help='Skip ETL and run forecasting only')
    args = parser.parse_args()

    print("\n" + "="*60)
    print("  SALES DATA PIPELINE & FORECASTING TOOL")
    print("="*60 + "\n")

    # Step 1 — Data generation
    txn_file = os.path.join(RAW_DIR, 'transactions.csv')
    if args.regenerate or not os.path.exists(txn_file):
        log.info("Step 1/3 — Generating raw data...")
        from generate_data import (
            N_TRANSACTIONS, customers, products, transactions
        )
    else:
        log.info("Step 1/3 — Raw data already exists, skipping generation.")
        log.info("           (use --regenerate to force regeneration)")

    if not args.forecast_only:
        # Step 2 — ETL
        log.info("\nStep 2/3 — Running ETL pipeline...")
        from etl_pipeline import run_etl
        run_etl()

    # Step 3 — Forecast
    log.info("\nStep 3/3 — Running revenue forecast...")
    from forecasting import run_forecast
    run_forecast(horizon=6)

    print("\n" + "="*60)
    print("  PIPELINE COMPLETE")
    print("="*60)
    print("\nOutputs:")
    print("  data/processed/   → Clean CSVs ready for Power BI")
    print("  data/exports/     → Forecast CSVs for Power BI charts")
    print("  sales_data.db     → SQLite database (PostgreSQL-compatible)")
    print("  reports/etl_log.txt → Full ETL audit log")
    print("\nPower BI Setup:")
    print("  1. Open Power BI Desktop")
    print("  2. Get Data → Text/CSV → select files from data/processed/")
    print("  3. Also import data/exports/revenue_actuals_vs_forecast.csv")
    print("  4. Build relationships on 'period', 'category', 'region' columns")
    print("  5. Create visuals using the KPI and summary tables\n")

if __name__ == '__main__':
    main()
