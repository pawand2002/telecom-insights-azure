-- ============================================================
-- TelecomInsights — Synapse Analytics Gold Layer
-- Run in: Synapse Studio → Develop → SQL Script
-- Pool: Serverless SQL Pool (free, no provisioning needed)
-- ============================================================

-- ── STEP 1: Create external data source pointing to ADLS Gold ─────────
-- Replace YOUR_STORAGE_ACCOUNT with your actual storage account name

CREATE DATABASE telecom_gold_synapse;
GO

USE telecom_gold_synapse;
GO

-- Create master key for encryption (run once)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'TelecomKV@2024!';
GO

-- Create credential using Managed Identity (ADF managed identity works here too)
CREATE DATABASE SCOPED CREDENTIAL ManagedIdentityCred
WITH IDENTITY = 'Managed Identity';
GO

-- Create external data source
CREATE EXTERNAL DATA SOURCE GoldAdlsSource
WITH (
    LOCATION   = 'abfss://gold@telecominsights7435.dfs.core.windows.net',
    CREDENTIAL = ManagedIdentityCred
);
GO

-- ── STEP 2: Create external file format for Delta Lake ────────────────
CREATE EXTERNAL FILE FORMAT DeltaFormat
WITH (FORMAT_TYPE = DELTA);
GO

-- ── STEP 3: Create views over Gold Delta Lake ───────────────

-- 3A. ARPU Monthly
CREATE OR ALTER VIEW gold_arpu_monthly AS
SELECT *
FROM OPENROWSET(
    BULK 'arpu_monthly/',
    DATA_SOURCE = 'GoldAdlsSource',
    FORMAT = 'DELTA'
) AS [result];
GO

-- 3B. Customer Summary
CREATE OR ALTER VIEW gold_customer_summary AS
SELECT *
FROM OPENROWSET(
    BULK 'customer_summary/',
    DATA_SOURCE = 'GoldAdlsSource',
    FORMAT = 'DELTA'
) AS [result];
GO

-- 3C. Regional KPIs
CREATE OR ALTER VIEW gold_regional_kpis AS
SELECT *
FROM OPENROWSET(
    BULK 'regional_kpis/',
    DATA_SOURCE = 'GoldAdlsSource',
    FORMAT = 'DELTA'
) AS [result];
GO

-- 3D. Daily Trend
CREATE OR ALTER VIEW gold_daily_trend AS
SELECT *
FROM OPENROWSET(
    BULK 'daily_trend/',
    DATA_SOURCE = 'GoldAdlsSource',
    FORMAT = 'DELTA'
) AS [result];
GO

-- 3E. Churn Scores
CREATE OR ALTER VIEW gold_churn_scores AS
SELECT *
FROM OPENROWSET(
    BULK 'churn_scores/',
    DATA_SOURCE = 'GoldAdlsSource',
    FORMAT = 'DELTA'
) AS [result];
GO

select top 10 * FROM gold_arpu_monthly;

select top 10 * FROM gold_customer_summary;

select top 10 * FROM gold_regional_kpis;

select top 10 * FROM gold_daily_trend;

select top 10 * FROM gold_churn_scores;

-- ── STEP 4: Power BI Optimised Views ─────────────────────────────────

-- View 1: Executive KPI Summary (Power BI KPI cards)
CREATE OR ALTER VIEW vw_executive_kpis AS
SELECT
    COUNT(DISTINCT customer_id)         AS total_customers,
    ROUND(AVG(avg_monthly_arpu), 2)     AS overall_arpu_qar,
    ROUND(SUM(total_cdr_revenue), 2)    AS total_revenue_qar,
    ROUND(AVG(call_drop_rate_pct), 2)   AS avg_drop_rate_pct,
    ROUND(AVG(total_data_gb), 3)        AS avg_data_gb_per_customer,
    SUM(CASE WHEN churn_risk = 'High' THEN 1 ELSE 0 END)   AS high_churn_customers,
    SUM(CASE WHEN is_high_value = 1 THEN 1 ELSE 0 END)     AS high_value_customers,
    ROUND(
        SUM(CASE WHEN churn_risk = 'High' THEN 1.0 ELSE 0 END)
        / COUNT(*) * 100, 2
    )                                   AS churn_risk_pct
FROM gold_customer_summary;
GO

-- View 2: ARPU Trend by Month (Power BI line chart)
CREATE OR ALTER VIEW vw_arpu_monthly_trend AS
SELECT
    call_month,
    segment,
    plan_type,
    region,
    COUNT(DISTINCT customer_id)         AS customer_count,
    ROUND(AVG(total_revenue_qar), 2)    AS avg_arpu,
    ROUND(SUM(total_revenue_qar), 2)    AS total_revenue,
    ROUND(AVG(call_drop_rate_pct), 2)   AS avg_drop_rate,
    ROUND(AVG(data_usage_gb), 3)        AS avg_data_gb,
    SUM(total_calls)                    AS total_calls,
    SUM(is_churning_signal)             AS churn_signals
FROM gold_arpu_monthly
GROUP BY call_month, segment, plan_type, region;
GO

-- View 3: Customer 360 (Power BI table + drill-through)
CREATE OR ALTER VIEW vw_customer_360 AS
SELECT
    cs.customer_id,
    cs.segment,
    cs.plan_type,
    cs.plan_name,
    cs.region,
    cs.tenure_band,
    cs.age_band,
    cs.churn_risk,
    cs.is_high_value,
    cs.monthly_fee_qar,
    cs.active_days,
    cs.total_cdrs,
    ROUND(cs.total_cdr_revenue, 2)      AS total_cdr_revenue,
    ROUND(cs.total_data_gb, 3)          AS total_data_gb,
    ROUND(cs.avg_call_duration_min, 2)      AS avg_call_duration_min,
    ROUND(cs.call_drop_rate_pct, 2)     AS call_drop_rate_pct,
    cs.total_recharges,
    ROUND(cs.total_recharge_amount, 2)  AS total_recharge_amount,
    ROUND(cs.avg_monthly_arpu, 2)       AS avg_monthly_arpu,
    -- Join churn ML score
    ROUND(ch.churn_probability * 100, 1) AS churn_probability_pct,
    ch.churn_score_band,
    ch.is_high_churn_risk
FROM gold_customer_summary cs
LEFT JOIN gold_churn_scores ch ON cs.customer_id = ch.customer_id;
GO

-- View 4: Regional performance (Power BI map + bar chart)
CREATE OR ALTER VIEW vw_regional_performance AS
SELECT
    region,
    SUM(customer_count)                 AS total_customers,
    ROUND(SUM(total_cdr_revenue), 2)    AS total_revenue,
    ROUND(AVG(avg_duration_min), 2)     AS avg_call_duration,
    ROUND(AVG(call_drop_rate_pct), 2)   AS avg_drop_rate,
    ROUND(SUM(total_data_gb), 2)        AS total_data_gb,
    ROUND(AVG(revenue_per_customer), 2) AS avg_revenue_per_customer
FROM gold_regional_kpis
GROUP BY region;
GO

-- View 5: Churn risk breakdown (Power BI donut + bar)
CREATE OR ALTER VIEW vw_churn_analysis AS
SELECT
    churn_score_band,
    COUNT(*)                                AS customer_count,
    ROUND(AVG(churn_probability), 4)        AS avg_churn_prob,
    ROUND(AVG(CAST(cs.tenure_months AS FLOAT)),1) AS avg_tenure_months,
    ROUND(AVG(cu.monthly_fee_qar), 2)          AS avg_monthly_fee,
    SUM(total_calls)                        AS total_calls,
    ROUND(AVG(drop_rate_pct), 2)            AS avg_drop_rate,
    SUM(cu.total_recharges)                    AS total_recharges
FROM gold_churn_scores cs
LEFT JOIN gold_customer_summary cu ON cs.customer_id = cu.customer_id
GROUP BY churn_score_band;
GO

-- ── STEP 5: Quick test queries ────────────────────────────────────────
-- Run these to verify everything works before connecting Power BI

SELECT * FROM vw_executive_kpis;

SELECT TOP 10 * FROM vw_arpu_monthly_trend ORDER BY call_month, total_revenue DESC;

SELECT TOP 20 * FROM vw_customer_360
ORDER BY avg_monthly_arpu DESC;

SELECT * FROM vw_regional_performance ORDER BY total_revenue DESC;

SELECT * FROM vw_churn_analysis ORDER BY avg_churn_prob DESC;
