# Databricks Notebook — 03_Silver_to_Gold_KPIs
# TelecomInsights — ARPU, Revenue, Usage KPIs
# Gold layer — Star schema ready for Synapse + Power BI
# =====================================================================

# ── CELL 1: Config ────────────────────────────────────────────────────
STORAGE_ACCOUNT = "telecominsights"   # ← change this

storage_key = dbutils.secrets.get(scope="telecom-kv", key="adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    storage_key
)

SILVER_CDR_PATH   = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/cdrs/"
SILVER_CUST_PATH  = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/customers/"
SILVER_RECH_PATH  = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/recharges/"
GOLD_PATH         = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/"

print("Paths configured")

# ── CELL 2: Load Silver data ──────────────────────────────────────────
df_cdr  = spark.read.format("delta").load(SILVER_CDR_PATH)
df_cust = spark.read.format("delta").load(SILVER_CUST_PATH)
df_rech = spark.read.format("delta").load(SILVER_RECH_PATH)

print(f"CDR rows:       {df_cdr.count():,}")
print(f"Customer rows:  {df_cust.count():,}")
print(f"Recharge rows:  {df_rech.count():,}")

# Register as temp views for SQL-style aggregations
df_cdr.createOrReplaceTempView("silver_cdr")
df_cust.createOrReplaceTempView("silver_customers")
df_rech.createOrReplaceTempView("silver_recharges")

# ── CELL 3: Gold — ARPU per Customer per Month ────────────────────────
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, round as spark_round,
    countDistinct, max as spark_max, min as spark_min,
    current_timestamp, lit, when, coalesce
)

# CDR revenue aggregated per customer per month
df_cdr_monthly = spark.sql("""
    SELECT
        customer_id,
        call_month,
        COUNT(*)                                        AS total_calls,
        SUM(CASE WHEN is_voice = 1 THEN 1 ELSE 0 END)  AS voice_calls,
        SUM(CASE WHEN is_data_session = 1 THEN 1 END)  AS data_sessions,
        SUM(CASE WHEN call_type = 'SMS' THEN 1 END)    AS sms_count,
        SUM(CASE WHEN is_dropped = 1 THEN 1 END)       AS dropped_calls,
        SUM(CASE WHEN is_roaming = 1 THEN 1 END)       AS roaming_calls,
        ROUND(SUM(duration_minutes), 2)                AS total_duration_min,
        ROUND(SUM(data_consumed_mb), 2)                AS total_data_mb,
        ROUND(SUM(revenue_qar), 3)                     AS cdr_revenue_qar,
        ROUND(AVG(duration_minutes), 2)                AS avg_call_duration_min,
        COUNT(DISTINCT network_type)                   AS network_types_used,
        MAX(call_date)                                 AS last_call_date
    FROM silver_cdr
    GROUP BY customer_id, call_month
""")

# Recharge revenue per customer per month
df_rech_monthly = spark.sql("""
    SELECT
        customer_id,
        recharge_month                                  AS call_month,
        COUNT(*)                                        AS recharge_count,
        SUM(recharge_amount)                            AS recharge_revenue_qar,
        AVG(recharge_amount)                            AS avg_recharge_amount,
        SUM(CASE WHEN is_digital_channel = 1 THEN 1 END) AS digital_recharges
    FROM silver_recharges
    GROUP BY customer_id, recharge_month
""")

# Join everything into Gold ARPU table
df_gold_arpu = (df_cdr_monthly
    .join(df_rech_monthly, ["customer_id","call_month"], "left")
    .join(df_cust.select(
        "customer_id","segment","plan_type","plan_name",
        "monthly_fee_qar","region","tenure_months",
        "age_band","tenure_band","is_high_value","churn_risk"
    ), "customer_id", "left")
    # Total ARPU = CDR revenue + recharge revenue + plan fee
    .withColumn("recharge_revenue_qar",
        coalesce(col("recharge_revenue_qar"), lit(0.0)))
    .withColumn("recharge_count",
        coalesce(col("recharge_count"), lit(0)))
    .withColumn("total_revenue_qar",
        spark_round(
            col("cdr_revenue_qar") +
            col("recharge_revenue_qar") +
            coalesce(col("monthly_fee_qar"), lit(0.0)), 2
        )
    )
    .withColumn("call_drop_rate_pct",
        spark_round(
            when(col("total_calls") > 0,
                col("dropped_calls") / col("total_calls") * 100
            ).otherwise(0.0), 2
        )
    )
    .withColumn("data_usage_gb",
        spark_round(col("total_data_mb") / 1024, 3))
    .withColumn("is_churning_signal",
        when(
            (col("churn_risk") == "High") &
            (col("total_calls") < 5), 1
        ).otherwise(0)
    )
    .withColumn("gold_load_dttm", current_timestamp())
)

gold_arpu_count = df_gold_arpu.count()
print(f"Gold ARPU rows: {gold_arpu_count:,}")
display(df_gold_arpu.limit(5))

# ── CELL 4: Gold — Customer Summary (one row per customer) ───────────
df_gold_customer_summary = spark.sql("""
    SELECT
        c.customer_id,
        c.segment,
        c.plan_type,
        c.plan_name,
        c.monthly_fee_qar,
        c.region,
        c.tenure_months,
        c.tenure_band,
        c.age_band,
        c.churn_risk,
        c.is_high_value,
        c.activation_date,

        -- CDR summary (all time)
        COUNT(DISTINCT cdr.call_date)       AS active_days,
        COUNT(cdr.cdr_id)                   AS total_cdrs,
        ROUND(SUM(cdr.revenue_qar), 2)      AS total_cdr_revenue,
        ROUND(SUM(cdr.data_consumed_mb)/1024, 3) AS total_data_gb,
        ROUND(AVG(cdr.duration_minutes), 2) AS avg_call_duration,
        ROUND(
            SUM(CASE WHEN cdr.is_dropped = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(cdr.cdr_id), 0), 2
        )                                   AS call_drop_rate_pct,

        -- Recharge summary
        COUNT(DISTINCT r.recharge_id)       AS total_recharges,
        ROUND(SUM(r.recharge_amount), 2)    AS total_recharge_amount,

        -- Overall ARPU (3-month average)
        ROUND((SUM(cdr.revenue_qar) + COALESCE(SUM(r.recharge_amount),0)
               + c.monthly_fee_qar * 3) / 3, 2) AS avg_monthly_arpu

    FROM silver_customers c
    LEFT JOIN silver_cdr cdr ON c.customer_id = cdr.customer_id
    LEFT JOIN silver_recharges r ON c.customer_id = r.customer_id
    GROUP BY
        c.customer_id, c.segment, c.plan_type, c.plan_name,
        c.monthly_fee_qar, c.region, c.tenure_months, c.tenure_band,
        c.age_band, c.churn_risk, c.is_high_value, c.activation_date
""")

print(f"Customer summary rows: {df_gold_customer_summary.count():,}")

# ── CELL 5: Gold — Regional KPI Summary ──────────────────────────────
df_gold_regional = spark.sql("""
    SELECT
        c.region,
        c.segment,
        COUNT(DISTINCT c.customer_id)               AS customer_count,
        COUNT(cdr.cdr_id)                           AS total_calls,
        ROUND(SUM(cdr.revenue_qar), 2)              AS total_cdr_revenue,
        ROUND(AVG(cdr.duration_minutes), 2)         AS avg_duration_min,
        ROUND(SUM(cdr.data_consumed_mb)/1024, 2)    AS total_data_gb,
        ROUND(
            SUM(CASE WHEN cdr.is_dropped = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(cdr.cdr_id), 0), 2
        )                                           AS call_drop_rate_pct,
        ROUND(SUM(cdr.revenue_qar) / COUNT(DISTINCT c.customer_id), 2)
                                                    AS revenue_per_customer
    FROM silver_customers c
    LEFT JOIN silver_cdr cdr ON c.customer_id = cdr.customer_id
    GROUP BY c.region, c.segment
    ORDER BY total_cdr_revenue DESC
""")

# ── CELL 6: Gold — Daily Trend ────────────────────────────────────────
df_gold_daily_trend = spark.sql("""
    SELECT
        call_date,
        call_month,
        COUNT(*)                                    AS total_calls,
        COUNT(DISTINCT customer_id)                 AS active_customers,
        ROUND(SUM(revenue_qar), 2)                  AS daily_revenue,
        ROUND(SUM(data_consumed_mb)/1024, 2)        AS daily_data_gb,
        ROUND(AVG(duration_minutes), 2)             AS avg_call_duration,
        SUM(CASE WHEN is_dropped = 1 THEN 1 END)    AS dropped_calls,
        SUM(CASE WHEN is_roaming = 1 THEN 1 END)    AS roaming_calls,
        SUM(CASE WHEN network_type = '5G' THEN 1 END) AS calls_5g,
        SUM(CASE WHEN network_type = '4G' THEN 1 END) AS calls_4g
    FROM silver_cdr
    GROUP BY call_date, call_month
    ORDER BY call_date
""")

# ── CELL 7: Write all Gold tables as Delta ────────────────────────────
tables = {
    "arpu_monthly":       (df_gold_arpu,             "call_month"),
    "customer_summary":   (df_gold_customer_summary, None),
    "regional_kpis":      (df_gold_regional,         None),
    "daily_trend":        (df_gold_daily_trend,      "call_month"),
}

for table_name, (df, partition_col) in tables.items():
    path = f"{GOLD_PATH}{table_name}/"
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema","true")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(path)
    print(f"✓ Gold/{table_name}: {df.count():,} rows → {path}")

print("\nAll Gold tables written!")

# ── CELL 8: Register Gold tables in Unity Catalog / Hive ─────────────
# This makes them queryable from Synapse Serverless SQL via external tables
spark.sql(f"""
    CREATE DATABASE IF NOT EXISTS telecom_gold
    LOCATION '{GOLD_PATH}'
""")

for table_name in tables.keys():
    path = f"{GOLD_PATH}{table_name}/"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS telecom_gold.{table_name}
        USING DELTA
        LOCATION '{path}'
    """)
    print(f"✓ Registered: telecom_gold.{table_name}")

# ── CELL 9: Final KPI print ───────────────────────────────────────────
print("\n========= GOLD KPI SUMMARY =========")
kpis = spark.sql("""
    SELECT
        COUNT(DISTINCT customer_id)             AS total_customers,
        ROUND(AVG(total_revenue_qar), 2)        AS avg_arpu_qar,
        ROUND(SUM(total_revenue_qar), 2)        AS total_revenue_qar,
        ROUND(AVG(call_drop_rate_pct), 2)       AS avg_drop_rate_pct,
        ROUND(AVG(data_usage_gb), 3)            AS avg_data_gb_per_customer,
        SUM(is_churning_signal)                 AS churn_risk_customers
    FROM telecom_gold.arpu_monthly
""").collect()[0]

print(f"  Total customers:        {kpis['total_customers']:,}")
print(f"  Avg ARPU (QAR):         {kpis['avg_arpu_qar']:,.2f}")
print(f"  Total revenue (QAR):    {kpis['total_revenue_qar']:,.2f}")
print(f"  Avg call drop rate:     {kpis['avg_drop_rate_pct']}%")
print(f"  Avg data usage:         {kpis['avg_data_gb_per_customer']} GB/customer")
print(f"  Churn risk customers:   {kpis['churn_risk_customers']:,}")

dbutils.notebook.exit("SUCCESS")
