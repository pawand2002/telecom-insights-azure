-- ============================================================
-- TelecomInsights — Synapse Analytics Gold Layer
-- Run in: Synapse Studio → Develop → SQL Script
-- Pool: Serverless SQL Pool (free, no provisioning needed)
-- ============================================================

-- ── STEP 1: Create external data source pointing to ADLS Gold ─────────
-- Replace YOUR_STORAGE_ACCOUNT with your actual storage account name

CREATE DATABASE IF NOT EXISTS telecom_gold_synapse;
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
    LOCATION   = 'abfss://gold@YOUR_STORAGE_ACCOUNT.dfs.core.windows.net',
    CREDENTIAL = ManagedIdentityCred
);
GO

-- ── STEP 2: Create external file format for Delta Lake ────────────────
CREATE EXTERNAL FILE FORMAT DeltaFormat
WITH (FORMAT_TYPE = DELTA);
GO

-- ── STEP 3: Create external tables over Gold Delta Lake ───────────────

-- 3A. ARPU Monthly
CREATE EXTERNAL TABLE gold_arpu_monthly (
    customer_id             VARCHAR(20),
    call_month              VARCHAR(10),
    total_calls             INT,
    voice_calls             INT,
    data_sessions           INT,
    sms_count               INT,
    dropped_calls           INT,
    roaming_calls           INT,
    total_duration_min      DECIMAL(12,2),
    total_data_mb           DECIMAL(12,2),
    cdr_revenue_qar         DECIMAL(12,3),
    avg_call_duration_min   DECIMAL(8,2),
    recharge_count          INT,
    recharge_revenue_qar    DECIMAL(12,2),
    avg_recharge_amount     DECIMAL(8,2),
    digital_recharges       INT,
    segment                 VARCHAR(20),
    plan_type               VARCHAR(20),
    plan_name               VARCHAR(50),
    monthly_fee_qar         DECIMAL(8,2),
    region                  VARCHAR(50),
    tenure_months           INT,
    age_band                VARCHAR(10),
    tenure_band             VARCHAR(10),
    is_high_value           INT,
    churn_risk              VARCHAR(10),
    total_revenue_qar       DECIMAL(12,2),
    call_drop_rate_pct      DECIMAL(6,2),
    data_usage_gb           DECIMAL(10,3),
    is_churning_signal      INT,
    gold_load_dttm          DATETIME2
)
WITH (
    LOCATION        = 'arpu_monthly/',
    DATA_SOURCE     = GoldAdlsSource,
    FILE_FORMAT     = DeltaFormat
);
GO

-- 3B. Customer Summary
CREATE EXTERNAL TABLE gold_customer_summary (
    customer_id             VARCHAR(20),
    segment                 VARCHAR(20),
    plan_type               VARCHAR(20),
    plan_name               VARCHAR(50),
    monthly_fee_qar         DECIMAL(8,2),
    region                  VARCHAR(50),
    tenure_months           INT,
    tenure_band             VARCHAR(10),
    age_band                VARCHAR(10),
    churn_risk              VARCHAR(10),
    is_high_value           INT,
    activation_date         DATE,
    active_days             INT,
    total_cdrs              INT,
    total_cdr_revenue       DECIMAL(12,2),
    total_data_gb           DECIMAL(10,3),
    avg_call_duration       DECIMAL(8,2),
    call_drop_rate_pct      DECIMAL(6,2),
    total_recharges         INT,
    total_recharge_amount   DECIMAL(12,2),
    avg_monthly_arpu        DECIMAL(10,2)
)
WITH (
    LOCATION        = 'customer_summary/',
    DATA_SOURCE     = GoldAdlsSource,
    FILE_FORMAT     = DeltaFormat
);
GO

-- 3C. Regional KPIs
CREATE EXTERNAL TABLE gold_regional_kpis (
    region                  VARCHAR(50),
    segment                 VARCHAR(20),
    customer_count          INT,
    total_calls             BIGINT,
    total_cdr_revenue       DECIMAL(14,2),
    avg_duration_min        DECIMAL(8,2),
    total_data_gb           DECIMAL(12,2),
    call_drop_rate_pct      DECIMAL(6,2),
    revenue_per_customer    DECIMAL(10,2)
)
WITH (
    LOCATION        = 'regional_kpis/',
    DATA_SOURCE     = GoldAdlsSource,
    FILE_FORMAT     = DeltaFormat
);
GO

-- 3D. Daily Trend
CREATE EXTERNAL TABLE gold_daily_trend (
    call_date               DATE,
    call_month              VARCHAR(10),
    total_calls             BIGINT,
    active_customers        INT,
    daily_revenue           DECIMAL(14,2),
    daily_data_gb           DECIMAL(12,2),
    avg_call_duration       DECIMAL(8,2),
    dropped_calls           INT,
    roaming_calls           INT,
    calls_5g                INT,
    calls_4g                INT
)
WITH (
    LOCATION        = 'daily_trend/',
    DATA_SOURCE     = GoldAdlsSource,
    FILE_FORMAT     = DeltaFormat
);
GO

-- 3E. Churn Scores
CREATE EXTERNAL TABLE gold_churn_scores (
    customer_id             VARCHAR(20),
    churn_probability       DECIMAL(6,4),
    churn_score_band        VARCHAR(20),
    is_high_churn_risk      INT,
    label                   INT,
    tenure_months           INT,
    monthly_fee_qar         DECIMAL(8,2),
    total_calls             INT,
    drop_rate_pct           DECIMAL(6,2),
    total_recharges         INT,
    active_days             INT,
    model_version           VARCHAR(20),
    score_dttm              DATETIME2,
    mlflow_run_id           VARCHAR(100)
)
WITH (
    LOCATION        = 'churn_scores/',
    DATA_SOURCE     = GoldAdlsSource,
    FILE_FORMAT     = DeltaFormat
);
GO

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
    ROUND(cs.avg_call_duration, 2)      AS avg_call_duration_min,
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
    ROUND(AVG(CAST(tenure_months AS FLOAT)),1) AS avg_tenure_months,
    ROUND(AVG(monthly_fee_qar), 2)          AS avg_monthly_fee,
    SUM(total_calls)                        AS total_calls,
    ROUND(AVG(drop_rate_pct), 2)            AS avg_drop_rate,
    SUM(total_recharges)                    AS total_recharges
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
