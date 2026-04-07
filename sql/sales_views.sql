-- =============================================================================
-- sales_views.sql
-- PostgreSQL-compatible analytical views
-- Run these on your Postgres instance after loading data via etl_pipeline.py
-- =============================================================================

-- ── 1. Monthly Revenue & KPIs ─────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_monthly_kpis AS
SELECT
    year,
    month,
    month_name,
    quarter,
    period,
    ROUND(total_revenue::NUMERIC, 2)        AS total_revenue,
    ROUND(total_profit::NUMERIC, 2)         AS total_profit,
    ROUND(total_cost::NUMERIC, 2)           AS total_cost,
    total_orders,
    total_units,
    ROUND(avg_order_value::NUMERIC, 2)      AS avg_order_value,
    ROUND(avg_margin_pct::NUMERIC, 2)       AS gross_margin_pct,
    unique_customers,
    high_value_orders,
    ROUND(return_rate_pct::NUMERIC, 2)      AS return_rate_pct,
    -- MoM Revenue Growth %
    ROUND(
        100.0 * (total_revenue - LAG(total_revenue) OVER (ORDER BY period))
        / NULLIF(LAG(total_revenue) OVER (ORDER BY period), 0),
    2) AS mom_revenue_growth_pct
FROM monthly_summary
ORDER BY period;

-- ── 2. Category Performance ───────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_category_performance AS
SELECT
    year,
    month,
    category,
    ROUND(revenue::NUMERIC, 2)   AS revenue,
    ROUND(profit::NUMERIC, 2)    AS profit,
    orders,
    units,
    ROUND(100.0 * profit / NULLIF(revenue, 0), 2) AS margin_pct
FROM category_summary
ORDER BY year, month, revenue DESC;

-- ── 3. Regional Heatmap ───────────────────────────────────────────────────────
CREATE OR REPLACE VIEW vw_regional_heatmap AS
SELECT
    year,
    month,
    region,
    segment,
    ROUND(revenue::NUMERIC, 2)     AS revenue,
    ROUND(profit::NUMERIC, 2)      AS profit,
    orders,
    customers,
    ROUND(revenue / NULLIF(customers, 0), 2) AS revenue_per_customer
FROM region_summary
ORDER BY year, month, revenue DESC;

-- ── 4. Channel & Campaign Effectiveness ──────────────────────────────────────
CREATE OR REPLACE VIEW vw_channel_campaign AS
SELECT
    year,
    month,
    channel,
    campaign,
    ROUND(revenue::NUMERIC, 2)          AS revenue,
    orders,
    ROUND(avg_discount::NUMERIC, 2)     AS avg_discount_pct,
    ROUND(total_discount::NUMERIC, 2)   AS total_discount_given,
    ROUND(revenue / NULLIF(orders, 0), 2) AS revenue_per_order
FROM channel_summary
ORDER BY year, month, revenue DESC;

-- ── 5. Top Products (from transactions) ───────────────────────────────────────
CREATE OR REPLACE VIEW vw_top_products AS
SELECT
    product_id,
    category,
    COUNT(*)                            AS total_orders,
    SUM(quantity)                       AS total_units_sold,
    ROUND(SUM(revenue)::NUMERIC, 2)     AS total_revenue,
    ROUND(SUM(profit)::NUMERIC, 2)      AS total_profit,
    ROUND(AVG(gross_margin_pct)::NUMERIC, 2) AS avg_margin_pct,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS total_returns
FROM transactions
WHERE status != 'Returned'
GROUP BY product_id, category
ORDER BY total_revenue DESC;

-- ── 6. Customer RFM (Recency-Frequency-Monetary) ─────────────────────────────
CREATE OR REPLACE VIEW vw_customer_rfm AS
SELECT
    customer_id,
    region,
    segment,
    COUNT(*)                                AS frequency,
    ROUND(SUM(revenue)::NUMERIC, 2)         AS monetary,
    ROUND(AVG(revenue)::NUMERIC, 2)         AS avg_order_value,
    MAX(date::DATE)                         AS last_purchase_date,
    CURRENT_DATE - MAX(date::DATE)          AS recency_days
FROM transactions
WHERE status != 'Returned'
GROUP BY customer_id, region, segment
ORDER BY monetary DESC;
