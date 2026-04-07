"""
etl_pipeline.py
---------------
Extract → Transform → Load pipeline.

Reads raw CSVs, performs data quality checks and transformations,
then loads clean data into a SQLite database (schema is fully
PostgreSQL-compatible — swap the connection string to deploy on Postgres).

Outputs:
  - Cleaned tables in sales_data.db
  - Processed CSVs in data/processed/ for Power BI import
  - ETL run log in reports/etl_log.txt
"""

import pandas as pd
import numpy as np
import sqlite3
import os
import logging
from datetime import datetime

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE        = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR     = os.path.join(BASE, 'data', 'raw')
PROC_DIR    = os.path.join(BASE, 'data', 'processed')
EXPORT_DIR  = os.path.join(BASE, 'data', 'exports')
REPORTS_DIR = os.path.join(BASE, 'reports')
DB_PATH     = os.path.join(BASE, 'sales_data.db')

for d in [PROC_DIR, EXPORT_DIR, REPORTS_DIR]:
    os.makedirs(d, exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)s  %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(REPORTS_DIR, 'etl_log.txt')),
        logging.StreamHandler(),
    ]
)
log = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
# EXTRACT
# ══════════════════════════════════════════════════════════════════════════════
def extract():
    log.info("=== EXTRACT ===")
    txn   = pd.read_csv(os.path.join(RAW_DIR, 'transactions.csv'), parse_dates=['date'])
    custs = pd.read_csv(os.path.join(RAW_DIR, 'customers.csv'))
    prods = pd.read_csv(os.path.join(RAW_DIR, 'products.csv'))
    log.info(f"Loaded {len(txn):,} transactions | {len(custs):,} customers | {len(prods):,} products")
    return txn, custs, prods

# ══════════════════════════════════════════════════════════════════════════════
# TRANSFORM
# ══════════════════════════════════════════════════════════════════════════════
def validate(df, name):
    """Run data quality checks, log issues, drop bad rows."""
    log.info(f"--- Validating {name} ---")
    before = len(df)

    # Null check
    nulls = df.isnull().sum()
    nulls = nulls[nulls > 0]
    if not nulls.empty:
        log.warning(f"  Nulls found:\n{nulls}")

    # Drop full duplicates
    df = df.drop_duplicates()

    # Transactions-specific checks
    if 'revenue' in df.columns:
        neg_rev = df[df['revenue'] < 0]
        if len(neg_rev):
            log.warning(f"  {len(neg_rev)} rows with negative revenue — dropping")
            df = df[df['revenue'] >= 0]

        zero_qty = df[df['quantity'] <= 0]
        if len(zero_qty):
            log.warning(f"  {len(zero_qty)} rows with zero/negative quantity — dropping")
            df = df[df['quantity'] > 0]

    after = len(df)
    log.info(f"  {before - after} rows removed in validation. {after:,} rows remain.")
    return df

def transform_transactions(txn, custs, prods):
    log.info("=== TRANSFORM ===")

    txn = validate(txn, 'transactions')

    # Date features
    txn['year']      = txn['date'].dt.year
    txn['month']     = txn['date'].dt.month
    txn['month_name']= txn['date'].dt.strftime('%b')
    txn['quarter']   = txn['date'].dt.quarter
    txn['week']      = txn['date'].dt.isocalendar().week.astype(int)
    txn['day_name']  = txn['date'].dt.day_name()
    txn['is_weekend']= txn['date'].dt.dayofweek >= 5

    # Enrich with customer & product data
    txn = txn.merge(
        custs[['customer_id', 'region', 'segment', 'city']],
        on='customer_id', how='left'
    )
    txn = txn.merge(
        prods[['product_id', 'category', 'cost_price']],
        on='product_id', how='left'
    )

    # Derived KPIs
    txn['gross_margin_pct'] = np.where(
        txn['revenue'] > 0,
        (txn['profit'] / txn['revenue'] * 100).round(2),
        0
    )
    txn['revenue_per_unit'] = (txn['revenue'] / txn['quantity']).round(2)

    # Flag high-value transactions (top 10%)
    threshold = txn['revenue'].quantile(0.90)
    txn['is_high_value'] = txn['revenue'] >= threshold

    log.info(f"  Enriched transactions: {len(txn):,} rows, {len(txn.columns)} columns")
    return txn

def build_monthly_summary(txn):
    """Monthly aggregated KPI table — primary Power BI source."""
    log.info("Building monthly summary...")
    agg = txn[txn['status'] != 'Returned'].groupby(
        ['year', 'month', 'month_name', 'quarter']
    ).agg(
        total_revenue     = ('revenue',          'sum'),
        total_profit      = ('profit',           'sum'),
        total_cost        = ('cost',             'sum'),
        total_orders      = ('transaction_id',   'count'),
        total_units       = ('quantity',         'sum'),
        avg_order_value   = ('revenue',          'mean'),
        avg_margin_pct    = ('gross_margin_pct', 'mean'),
        unique_customers  = ('customer_id',      'nunique'),
        high_value_orders = ('is_high_value',    'sum'),
        returns           = ('is_returned',      'sum'),
    ).reset_index()

    agg['total_revenue']    = agg['total_revenue'].round(2)
    agg['total_profit']     = agg['total_profit'].round(2)
    agg['avg_order_value']  = agg['avg_order_value'].round(2)
    agg['avg_margin_pct']   = agg['avg_margin_pct'].round(2)
    agg['return_rate_pct']  = (agg['returns'] / agg['total_orders'] * 100).round(2)

    # Month label for sorting
    agg['period'] = agg['year'].astype(str) + '-' + agg['month'].astype(str).str.zfill(2)
    agg = agg.sort_values('period').reset_index(drop=True)
    log.info(f"  Monthly summary: {len(agg)} rows")
    return agg

def build_category_summary(txn):
    log.info("Building category summary...")
    agg = txn[txn['status'] != 'Returned'].groupby(
        ['year', 'month', 'category']
    ).agg(
        revenue = ('revenue', 'sum'),
        profit  = ('profit',  'sum'),
        orders  = ('transaction_id', 'count'),
        units   = ('quantity', 'sum'),
    ).reset_index().round(2)
    log.info(f"  Category summary: {len(agg)} rows")
    return agg

def build_region_summary(txn):
    log.info("Building region summary...")
    agg = txn[txn['status'] != 'Returned'].groupby(
        ['year', 'month', 'region', 'segment']
    ).agg(
        revenue  = ('revenue', 'sum'),
        profit   = ('profit',  'sum'),
        orders   = ('transaction_id', 'count'),
        customers= ('customer_id', 'nunique'),
    ).reset_index().round(2)
    log.info(f"  Region summary: {len(agg)} rows")
    return agg

def build_channel_campaign_summary(txn):
    log.info("Building channel/campaign summary...")
    agg = txn[txn['status'] != 'Returned'].groupby(
        ['year', 'month', 'channel', 'campaign']
    ).agg(
        revenue        = ('revenue', 'sum'),
        orders         = ('transaction_id', 'count'),
        avg_discount   = ('discount_pct', 'mean'),
        total_discount = ('discount_amount', 'sum'),
    ).reset_index().round(2)
    log.info(f"  Channel/campaign summary: {len(agg)} rows")
    return agg

# ══════════════════════════════════════════════════════════════════════════════
# LOAD
# ══════════════════════════════════════════════════════════════════════════════
def load(txn, custs, prods, monthly, category, region, channel):
    log.info("=== LOAD ===")

    # ── SQLite (PostgreSQL-compatible schema) ──────────────────────────────────
    conn = sqlite3.connect(DB_PATH)
    log.info(f"Connected to DB: {DB_PATH}")

    tables = {
        'transactions':       txn,
        'customers':          custs,
        'products':           prods,
        'monthly_summary':    monthly,
        'category_summary':   category,
        'region_summary':     region,
        'channel_summary':    channel,
    }
    for name, df in tables.items():
        df.to_sql(name, conn, if_exists='replace', index=False)
        log.info(f"  Loaded table '{name}': {len(df):,} rows")

    conn.close()

    # ── Processed CSVs (Power BI import) ─────────────────────────────────────
    monthly.to_csv(os.path.join(PROC_DIR, 'monthly_summary.csv'),  index=False)
    category.to_csv(os.path.join(PROC_DIR, 'category_summary.csv'), index=False)
    region.to_csv(os.path.join(PROC_DIR,   'region_summary.csv'),   index=False)
    channel.to_csv(os.path.join(PROC_DIR,  'channel_summary.csv'),  index=False)
    txn.to_csv(os.path.join(PROC_DIR,      'transactions_clean.csv'), index=False)
    log.info("  Processed CSVs saved to data/processed/")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def run_etl():
    log.info(f"\n{'='*60}")
    log.info(f"ETL RUN STARTED  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'='*60}")

    txn, custs, prods    = extract()
    txn_clean            = transform_transactions(txn, custs, prods)
    monthly              = build_monthly_summary(txn_clean)
    category             = build_category_summary(txn_clean)
    region               = build_region_summary(txn_clean)
    channel              = build_channel_campaign_summary(txn_clean)
    load(txn_clean, custs, prods, monthly, category, region, channel)

    log.info(f"\nETL COMPLETE  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'='*60}\n")
    return monthly   # return for forecasting module

if __name__ == '__main__':
    run_etl()
