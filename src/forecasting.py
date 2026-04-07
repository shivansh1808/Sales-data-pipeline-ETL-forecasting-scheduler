"""
forecasting.py
--------------
Revenue forecasting module using:
  1. Moving Average (3-month and 6-month)
  2. Linear Regression (trend-based)
  3. Combined ensemble forecast

Reads from data/processed/monthly_summary.csv
Outputs forecast CSVs to data/exports/ for Power BI overlay charts.
Prints model accuracy metrics.
"""

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
import os
import logging

BASE       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROC_DIR   = os.path.join(BASE, 'data', 'processed')
EXPORT_DIR = os.path.join(BASE, 'data', 'exports')
os.makedirs(EXPORT_DIR, exist_ok=True)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s  %(levelname)s  %(message)s')

# ══════════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ══════════════════════════════════════════════════════════════════════════════
def load_monthly():
    path = os.path.join(PROC_DIR, 'monthly_summary.csv')
    df   = pd.read_csv(path)
    df   = df.sort_values('period').reset_index(drop=True)
    df['t'] = range(len(df))   # numeric time index
    log.info(f"Loaded {len(df)} monthly periods ({df['period'].iloc[0]} → {df['period'].iloc[-1]})")
    return df

# ══════════════════════════════════════════════════════════════════════════════
# FEATURE ENGINEERING
# ══════════════════════════════════════════════════════════════════════════════
def add_features(df):
    df = df.copy()
    df['ma_3']         = df['total_revenue'].rolling(3, min_periods=1).mean()
    df['ma_6']         = df['total_revenue'].rolling(6, min_periods=1).mean()
    df['lag_1']        = df['total_revenue'].shift(1)
    df['lag_3']        = df['total_revenue'].shift(3)
    df['sin_month']    = np.sin(2 * np.pi * df['month'] / 12)   # seasonality
    df['cos_month']    = np.cos(2 * np.pi * df['month'] / 12)
    df['quarter_enc']  = df['quarter']
    return df

# ══════════════════════════════════════════════════════════════════════════════
# MOVING AVERAGE FORECAST
# ══════════════════════════════════════════════════════════════════════════════
def moving_average_forecast(df, window=3, horizon=6):
    """Simple rolling moving average extrapolation."""
    last_ma  = df['total_revenue'].iloc[-window:].mean()
    forecasts= []
    history  = list(df['total_revenue'])

    for i in range(horizon):
        next_val = np.mean(history[-window:])
        forecasts.append(round(next_val, 2))
        history.append(next_val)

    return forecasts

# ══════════════════════════════════════════════════════════════════════════════
# LINEAR REGRESSION FORECAST
# ══════════════════════════════════════════════════════════════════════════════
def linear_regression_forecast(df, horizon=6):
    """
    Train linear regression on historical monthly revenue.
    Features: time index, seasonality (sin/cos month), quarter.
    Uses last 20% of data as hold-out test set to compute accuracy.
    """
    df = add_features(df).dropna()

    features = ['t', 'sin_month', 'cos_month', 'quarter_enc', 'ma_3', 'lag_1']
    X = df[features].values
    y = df['total_revenue'].values

    # Train/test split — last 20% as test
    split    = int(len(df) * 0.80)
    X_train, X_test = X[:split], X[split:]
    y_train, y_test = y[:split], y[split:]

    scaler  = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test  = scaler.transform(X_test)

    model = LinearRegression()
    model.fit(X_train, y_train)

    # ── Accuracy metrics ──────────────────────────────────────────────────────
    y_pred = model.predict(X_test)
    mae    = mean_absolute_error(y_test, y_pred)
    rmse   = np.sqrt(mean_squared_error(y_test, y_pred))
    r2     = r2_score(y_test, y_pred)
    mape   = np.mean(np.abs((y_test - y_pred) / y_test)) * 100

    log.info("── Linear Regression Accuracy (Hold-out Test Set) ──")
    log.info(f"   MAE   : ₹{mae:,.0f}")
    log.info(f"   RMSE  : ₹{rmse:,.0f}")
    log.info(f"   R²    : {r2:.4f}")
    log.info(f"   MAPE  : {mape:.2f}%  →  Accuracy: {100-mape:.2f}%")

    # ── Forecast next N months ────────────────────────────────────────────────
    last_row    = df.iloc[-1]
    last_t      = int(last_row['t'])
    last_month  = int(last_row['month'])
    last_quarter= int(last_row['quarter'])
    last_ma3    = float(last_row['ma_3'])
    last_lag1   = float(last_row['total_revenue'])

    future_rows = []
    for i in range(1, horizon + 1):
        m = ((last_month - 1 + i) % 12) + 1
        q = (m - 1) // 3 + 1
        row = [
            last_t + i,
            np.sin(2 * np.pi * m / 12),
            np.cos(2 * np.pi * m / 12),
            q,
            last_ma3,
            last_lag1,
        ]
        future_rows.append(row)

    X_future   = scaler.transform(np.array(future_rows))
    forecasts  = model.predict(X_future)
    forecasts  = [round(max(f, 0), 2) for f in forecasts]

    return forecasts, r2, mape, model

# ══════════════════════════════════════════════════════════════════════════════
# BUILD FORECAST OUTPUT
# ══════════════════════════════════════════════════════════════════════════════
def build_forecast_df(df, horizon=6):
    """Combine MA and LR forecasts into a single export DataFrame."""
    log.info(f"\nGenerating {horizon}-month forecast...")

    ma_forecasts, *_          = moving_average_forecast(df, window=3, horizon=horizon), None
    # unpack cleanly
    ma_fc   = moving_average_forecast(df, window=3, horizon=horizon)
    lr_fc, r2, mape, model    = linear_regression_forecast(df, horizon=horizon)

    # Ensemble: weighted average (LR weighted higher if R² > 0.8)
    w_lr = 0.65 if r2 > 0.8 else 0.50
    w_ma = 1 - w_lr
    ensemble = [round(w_lr * lr + w_ma * ma, 2)
                for lr, ma in zip(lr_fc, ma_fc)]

    # Generate future period labels
    last_period = df['period'].iloc[-1]
    last_year, last_month = int(last_period.split('-')[0]), int(last_period.split('-')[1])
    future_periods = []
    for i in range(1, horizon + 1):
        m = last_month + i
        y = last_year + (m - 1) // 12
        m = ((m - 1) % 12) + 1
        future_periods.append(f'{y}-{str(m).zfill(2)}')

    forecast_df = pd.DataFrame({
        'period':            future_periods,
        'type':              'Forecast',
        'ma_3_forecast':     ma_fc,
        'lr_forecast':       lr_fc,
        'ensemble_forecast': ensemble,
    })

    # Historical actuals for comparison chart
    actuals_df = df[['period', 'total_revenue']].copy()
    actuals_df['type'] = 'Actual'
    actuals_df = actuals_df.rename(columns={'total_revenue': 'revenue'})

    forecast_export = forecast_df.rename(columns={'ensemble_forecast': 'revenue'})
    forecast_export['revenue'] = forecast_export['revenue']

    combined = pd.concat([
        actuals_df[['period', 'type', 'revenue']],
        forecast_export[['period', 'type', 'revenue']]
    ], ignore_index=True)

    return combined, forecast_df, r2, mape

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def run_forecast(horizon=6):
    df = load_monthly()
    combined, forecast_df, r2, mape = build_forecast_df(df, horizon=horizon)

    # Save exports
    combined.to_csv(os.path.join(EXPORT_DIR, 'revenue_actuals_vs_forecast.csv'), index=False)
    forecast_df.to_csv(os.path.join(EXPORT_DIR, 'forecast_detail.csv'), index=False)
    log.info(f"\nForecast exports saved to data/exports/")
    log.info(f"  revenue_actuals_vs_forecast.csv  ← use in Power BI line chart")
    log.info(f"  forecast_detail.csv              ← MA vs LR vs Ensemble comparison")

    # Summary
    log.info(f"\n{'='*50}")
    log.info(f"FORECAST SUMMARY")
    log.info(f"  Model R²       : {r2:.4f}")
    log.info(f"  Model MAPE     : {mape:.2f}%")
    log.info(f"  Model Accuracy : ~{100-mape:.1f}%")
    log.info(f"{'='*50}")

    print("\n── 6-Month Revenue Forecast ──")
    print(forecast_df[['period', 'ma_3_forecast', 'lr_forecast', 'ensemble_forecast']].to_string(index=False))

    return combined

if __name__ == '__main__':
    run_forecast(horizon=6)
