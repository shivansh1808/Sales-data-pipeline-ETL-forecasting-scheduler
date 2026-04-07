"""
generate_data.py
----------------
Generates 50,000+ realistic transactional sales records and saves them
as raw CSV files in data/raw/. Mimics a real retail/banking sales dataset
with customers, products, regions, channels, and campaign tags.
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker('en_IN')
np.random.seed(42)
random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
N_TRANSACTIONS  = 52000
N_CUSTOMERS     = 3000
N_PRODUCTS      = 80
START_DATE      = datetime(2022, 1, 1)
END_DATE        = datetime(2024, 12, 31)
OUTPUT_DIR      = os.path.join(os.path.dirname(__file__), '..', 'data', 'raw')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Reference data ────────────────────────────────────────────────────────────
REGIONS   = ['North', 'South', 'East', 'West', 'Central']
CHANNELS  = ['Online', 'In-Store', 'Mobile App', 'Telesales']
SEGMENTS  = ['Retail', 'SME', 'Corporate', 'Premium']
CAMPAIGNS = ['Summer Sale', 'Diwali Offer', 'New Year Push',
             'Back to School', 'End of Season', 'None']

CATEGORIES = {
    'Electronics':   (5000,  80000,  0.12),
    'Clothing':      (500,   8000,   0.08),
    'Home Appliance':(3000,  60000,  0.10),
    'Financial Prod':(1000,  50000,  0.05),
    'Accessories':   (200,   3000,   0.15),
    'Sports':        (800,   20000,  0.09),
    'Books':         (150,   1500,   0.20),
    'Food & Bev':    (100,   2000,   0.25),
}

# ── Customers ──────────────────────────────────────────────────────────────────
print("Generating customers...")
customers = pd.DataFrame({
    'customer_id':  [f'CUST{str(i).zfill(5)}' for i in range(1, N_CUSTOMERS + 1)],
    'name':         [fake.name() for _ in range(N_CUSTOMERS)],
    'city':         [fake.city() for _ in range(N_CUSTOMERS)],
    'region':       np.random.choice(REGIONS, N_CUSTOMERS),
    'segment':      np.random.choice(SEGMENTS, N_CUSTOMERS, p=[0.50, 0.25, 0.15, 0.10]),
    'join_date':    [fake.date_between(start_date='-5y', end_date='-1y')
                     for _ in range(N_CUSTOMERS)],
    'email':        [fake.email() for _ in range(N_CUSTOMERS)],
})
customers.to_csv(os.path.join(OUTPUT_DIR, 'customers.csv'), index=False)
print(f"  → {len(customers)} customers saved")

# ── Products ──────────────────────────────────────────────────────────────────
print("Generating products...")
product_rows = []
pid = 1
for cat, (price_min, price_max, return_rate) in CATEGORIES.items():
    n = N_PRODUCTS // len(CATEGORIES)
    for _ in range(n):
        product_rows.append({
            'product_id':   f'PROD{str(pid).zfill(4)}',
            'product_name': f'{fake.word().capitalize()} {cat} {pid}',
            'category':     cat,
            'unit_price':   round(random.uniform(price_min, price_max), 2),
            'cost_price':   round(random.uniform(price_min * 0.5, price_max * 0.7), 2),
            'return_rate':  return_rate,
        })
        pid += 1

products = pd.DataFrame(product_rows)
products.to_csv(os.path.join(OUTPUT_DIR, 'products.csv'), index=False)
print(f"  → {len(products)} products saved")

# ── Transactions ───────────────────────────────────────────────────────────────
print("Generating transactions...")

def random_date(start, end):
    delta = end - start
    # Weight towards recent months (simulate growth)
    weights = np.linspace(0.5, 1.5, delta.days)
    weights /= weights.sum()
    day_offset = np.random.choice(delta.days, p=weights)
    return start + timedelta(days=int(day_offset))

cust_ids = customers['customer_id'].tolist()
prod_ids = products['product_id'].tolist()

rows = []
for i in range(N_TRANSACTIONS):
    pid_  = random.choice(prod_ids)
    prod  = products[products['product_id'] == pid_].iloc[0]
    qty   = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 12, 8, 5])[0]
    price = prod['unit_price']
    # Apply occasional discount
    discount_pct = random.choices([0, 5, 10, 15, 20], weights=[55, 20, 12, 8, 5])[0]
    discount_amt = round(price * qty * discount_pct / 100, 2)
    revenue      = round(price * qty - discount_amt, 2)
    cost         = round(prod['cost_price'] * qty, 2)
    profit       = round(revenue - cost, 2)
    is_returned  = random.random() < prod['return_rate']

    rows.append({
        'transaction_id':  f'TXN{str(i+1).zfill(7)}',
        'date':            random_date(START_DATE, END_DATE).strftime('%Y-%m-%d'),
        'customer_id':     random.choice(cust_ids),
        'product_id':      pid_,
        'channel':         random.choice(CHANNELS),
        'campaign':        random.choice(CAMPAIGNS),
        'quantity':        qty,
        'unit_price':      price,
        'discount_pct':    discount_pct,
        'discount_amount': discount_amt,
        'revenue':         revenue,
        'cost':            cost,
        'profit':          profit,
        'is_returned':     is_returned,
        'status':          'Returned' if is_returned else random.choice(
                               ['Completed', 'Completed', 'Completed', 'Pending']),
    })

transactions = pd.DataFrame(rows)
transactions['date'] = pd.to_datetime(transactions['date'])
transactions.to_csv(os.path.join(OUTPUT_DIR, 'transactions.csv'), index=False)
print(f"  → {len(transactions)} transactions saved")
print("\nRaw data generation complete.")
