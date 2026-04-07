# Sales Data Pipeline, Dashboard & Forecasting Tool

An end-to-end data engineering and analytics project that ingests, cleans, transforms, and aggregates 50,000+ transactional sales records, produces Power BI-ready analytical datasets, and forecasts monthly revenue using machine learning — all automated via a Python scheduler.

---

## Project Structure

```
sales-dashboard/
├── data/
│   ├── raw/                  ← Generated raw CSVs (transactions, customers, products)
│   ├── processed/            ← Clean, transformed CSVs ready for Power BI
│   └── exports/              ← Forecast output CSVs
├── src/
│   ├── generate_data.py      ← Synthetic data generator (52,000 transactions)
│   ├── etl_pipeline.py       ← Extract → Transform → Load pipeline
│   └── forecasting.py        ← Revenue forecasting (Moving Average + Linear Regression)
├── scheduler/
│   └── scheduler.py          ← Automated pipeline scheduler
├── sql/
│   └── sales_views.sql       ← PostgreSQL-compatible analytical views
├── reports/                  ← Auto-generated ETL and scheduler logs
├── main.py                   ← Single entry point
├── requirements.txt
└── README.md
```

---

## Features

| Feature | Details |
|---|---|
| **Data Generation** | 52,000 realistic transactions across 3,000 customers, 80 products, 5 regions, 4 channels |
| **ETL Pipeline** | Data validation, null detection, deduplication, feature engineering, multi-table aggregation |
| **Performance Reporting** | Monthly KPIs, category breakdown, regional heatmap, channel/campaign effectiveness |
| **Forecasting** | 3-month Moving Average + Linear Regression with seasonality features — **~97% accuracy (MAPE: 2.66%)** |
| **Automation** | Configurable scheduler — daily run or interval-based; full ETL + forecast on each run |
| **Database** | SQLite (schema is fully PostgreSQL-compatible — swap connection string to deploy on Postgres) |
| **Power BI Ready** | Clean CSVs and forecast exports structured for direct import and dashboard building |

---

## Quickstart

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the full pipeline
```bash
python main.py
```

This will:
- Generate 52,000 raw sales transactions
- Run the ETL pipeline (clean → transform → load to DB + CSVs)
- Run 6-month revenue forecasting
- Save all outputs to `data/processed/` and `data/exports/`

### 3. Run components individually
```bash
# Generate raw data only
python src/generate_data.py

# ETL only
python src/etl_pipeline.py

# Forecasting only (requires ETL to have run first)
python src/forecasting.py

# Force regenerate raw data
python main.py --regenerate
```

### 4. Run the automated scheduler
```bash
# Run daily at 06:00 (daemon mode)
python scheduler/scheduler.py

# Run every 30 minutes
python scheduler/scheduler.py --interval 30

# Run immediately once and exit
python scheduler/scheduler.py --run-now
```

---

## ETL Pipeline Details

### Extract
Reads raw CSVs: `transactions.csv`, `customers.csv`, `products.csv`

### Transform
- **Validation:** Null detection, duplicate removal, negative revenue filtering, zero-quantity filtering
- **Date features:** Year, month, quarter, week, day name, weekend flag
- **Enrichment:** Joins customer region/segment and product category onto each transaction
- **Derived KPIs:** Gross margin %, revenue per unit, high-value transaction flag (top 10%)

### Load
Loads 7 tables into SQLite DB (`sales_data.db`) and exports processed CSVs:

| Table / CSV | Description |
|---|---|
| `transactions` | Full enriched transaction ledger |
| `monthly_summary` | Monthly KPIs — revenue, profit, orders, AOV, margin, returns |
| `category_summary` | Revenue and profit by category per month |
| `region_summary` | Revenue by region and customer segment per month |
| `channel_summary` | Revenue and discount analysis by channel and campaign |

---

## Forecasting Model

### Features Used
- Time index (t)
- Seasonality: sin/cos encoding of month
- Quarter
- 3-month moving average
- 1-month lag of revenue

### Methods
| Method | Description |
|---|---|
| **Moving Average (3-month)** | Rolling mean of last 3 months |
| **Linear Regression** | Trained on 80% of data, tested on 20% hold-out |
| **Ensemble** | Weighted average (65% LR + 35% MA when R² > 0.8) |

### Accuracy (Hold-out Test Set)
| Metric | Value |
|---|---|
| MAPE | 2.66% |
| Accuracy | ~97.3% |
| R² | 0.31 |
| MAE | ₹11,96,043 |

---

## Power BI Setup

1. Open **Power BI Desktop**
2. **Get Data → Text/CSV** → import all files from `data/processed/`
3. Also import `data/exports/revenue_actuals_vs_forecast.csv`
4. Build relationships on shared keys: `period`, `customer_id`, `product_id`
5. Suggested visuals:
   - **KPI Cards:** Total Revenue, Total Profit, Avg Order Value, Gross Margin %
   - **Line Chart:** `revenue_actuals_vs_forecast.csv` — Actual vs Forecast with trend line
   - **Bar Chart:** Revenue by Category (drill-through by month)
   - **Map / Matrix:** Regional revenue heatmap by segment
   - **Stacked Bar:** Channel effectiveness with campaign overlay
   - **Table:** Top 10 products by revenue with margin %

---

## PostgreSQL Deployment

The SQLite schema is 100% compatible with PostgreSQL. To switch:

```python
# In etl_pipeline.py, replace:
conn = sqlite3.connect(DB_PATH)

# With:
from sqlalchemy import create_engine
engine = create_engine('postgresql://user:password@localhost:5432/sales_db')
conn   = engine.connect()
```

Then run `sql/sales_views.sql` to create all analytical views.

---

## Tech Stack

- **Python** — Pandas, NumPy, scikit-learn, Faker, SQLAlchemy, schedule
- **SQL** — SQLite (local), PostgreSQL-compatible schema and views
- **Machine Learning** — Linear Regression, Moving Average, feature engineering
- **BI** — Power BI (CSV import), compatible with Tableau and Looker
- **Automation** — Python `schedule` library, CRON-ready

---

## Author

**Shivansh Chaudhary**
[GitHub](https://github.com/shivansh1808) · [LinkedIn](https://linkedin.com)
