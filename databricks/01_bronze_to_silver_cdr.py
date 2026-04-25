# Databricks Notebook — 01_Bronze_to_Silver_CDR
# TelecomInsights — CDR Cleansing + Feature Engineering
# Run: manually first, then schedule via ADF Databricks Notebook Activity
# =====================================================================

# ── CELL 1: Configuration ─────────────────────────────────────────────
# Get parameters passed from ADF (or use defaults for manual run)
dbutils.widgets.text("window_start", "2024-10-01", "Window Start Date")
dbutils.widgets.text("window_end",   "2024-12-31", "Window End Date")
dbutils.widgets.text("env",          "dev",         "Environment")

WINDOW_START  = dbutils.widgets.get("window_start")
WINDOW_END    = dbutils.widgets.get("window_end")
ENV           = dbutils.widgets.get("env")

# Storage paths — replace with your actual storage account name
STORAGE_ACCOUNT = "telecominsights"   # ← change this

BRONZE_CDR_PATH  = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/cdrs/"
SILVER_CDR_PATH  = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/cdrs/"

print(f"Environment:  {ENV}")
print(f"Window:       {WINDOW_START} → {WINDOW_END}")
print(f"Bronze path:  {BRONZE_CDR_PATH}")
print(f"Silver path:  {SILVER_CDR_PATH}")

# ── CELL 2: Mount ADLS (run once — skip if already mounted) ───────────
# Option A: Using Account Key (simpler for dev)
# Get key from Key Vault — never hardcode!
storage_key = dbutils.secrets.get(scope="telecom-kv", key="adls-storage-key")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    storage_key
)
print("Storage account configured")

# Option B: Using Service Principal (recommended for production)
# spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net",
#                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net",
#                dbutils.secrets.get("telecom-kv", "sp-client-id"))
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net",
#                dbutils.secrets.get("telecom-kv", "sp-client-secret"))
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net",
#                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# ── CELL 3: Read Bronze CDR files ─────────────────────────────────────
from pyspark.sql.types import (StructType, StructField, StringType,
                                IntegerType, DoubleType, TimestampType)

# Always define schema explicitly — never inferSchema in production
cdr_schema = StructType([
    StructField("cdr_id",            StringType(),    False),
    StructField("customer_id",       StringType(),    False),
    StructField("msisdn",            StringType(),    True),
    StructField("call_type",         StringType(),    True),
    StructField("call_start_dttm",   StringType(),    True),
    StructField("call_end_dttm",     StringType(),    True),
    StructField("duration_seconds",  IntegerType(),   True),
    StructField("network_type",      StringType(),    True),
    StructField("data_consumed_mb",  DoubleType(),    True),
    StructField("termination_code",  StringType(),    True),
    StructField("is_roaming",        IntegerType(),   True),
    StructField("roaming_country",   StringType(),    True),
    StructField("revenue_qar",       DoubleType(),    True),
    StructField("plan_type",         StringType(),    True),
    StructField("plan_name",         StringType(),    True),
    StructField("region",            StringType(),    True),
    StructField("segment",           StringType(),    True),
    StructField("call_date",         StringType(),    True),
    StructField("call_hour",         IntegerType(),   True),
    StructField("created_at",        StringType(),    True),
])

# Read all Bronze CDR files within window
df_bronze = (spark.read
    .option("header", "true")
    .schema(cdr_schema)
    .csv(BRONZE_CDR_PATH)
    .filter(f"call_date >= '{WINDOW_START}' AND call_date <= '{WINDOW_END}'")
)

raw_count = df_bronze.count()
print(f"Bronze CDRs read: {raw_count:,}")
display(df_bronze.limit(5))

# ── CELL 4: Data Quality Checks ───────────────────────────────────────
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum

print("=== DATA QUALITY REPORT ===")

# Null check on mandatory columns
mandatory_cols = ["cdr_id","customer_id","call_type","call_start_dttm","call_date"]
for c in mandatory_cols:
    null_count = df_bronze.filter(col(c).isNull()).count()
    status = "✓ OK" if null_count == 0 else f"⚠ {null_count} nulls"
    print(f"  {c:25s}: {status}")

# Negative revenue check
neg_revenue = df_bronze.filter(col("revenue_qar") < 0).count()
print(f"  {'negative revenue':25s}: {neg_revenue} rows")

# Duplicate CDR IDs
total     = df_bronze.count()
distinct  = df_bronze.select("cdr_id").distinct().count()
dupe_count= total - distinct
print(f"  {'duplicate cdr_ids':25s}: {dupe_count} duplicates")

print(f"\nTotal rows: {total:,} | Distinct: {distinct:,}")

# ── CELL 5: Silver Transformation ─────────────────────────────────────
from pyspark.sql.functions import (
    col, to_timestamp, to_date, upper, lower, trim,
    when, lit, current_timestamp, round as spark_round,
    regexp_replace, coalesce, floor, expr
)
from pyspark.sql.types import DecimalType

df_silver = (df_bronze
    # ── Type casting ──────────────────────────────────────────────
    .withColumn("call_start_dttm",
        to_timestamp(col("call_start_dttm"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("call_end_dttm",
        to_timestamp(col("call_end_dttm"),   "yyyy-MM-dd HH:mm:ss"))
    .withColumn("call_date",
        to_date(col("call_date"), "yyyy-MM-dd"))
    .withColumn("revenue_qar",
        col("revenue_qar").cast(DecimalType(10, 3)))
    .withColumn("data_consumed_mb",
        col("data_consumed_mb").cast(DecimalType(12, 2)))

    # ── Standardise string columns ────────────────────────────────
    .withColumn("call_type",        upper(trim(col("call_type"))))
    .withColumn("network_type",     upper(trim(col("network_type"))))
    .withColumn("termination_code", upper(trim(col("termination_code"))))
    .withColumn("region",           trim(col("region")))
    .withColumn("segment",          trim(col("segment")))

    # ── Null handling ─────────────────────────────────────────────
    .withColumn("revenue_qar",
        coalesce(col("revenue_qar"), lit(0.0)))
    .withColumn("data_consumed_mb",
        coalesce(col("data_consumed_mb"), lit(0.0)))
    .withColumn("roaming_country",
        coalesce(col("roaming_country"), lit("DOMESTIC")))

    # ── Derived features (Silver enrichment) ─────────────────────
    .withColumn("duration_minutes",
        spark_round(col("duration_seconds") / 60.0, 2))
    .withColumn("is_dropped",
        when(col("termination_code") == "USER_DROPPED", 1).otherwise(0))
    .withColumn("is_successful",
        when(col("termination_code") == "NORMAL", 1).otherwise(0))
    .withColumn("is_data_session",
        when(col("call_type") == "DATA", 1).otherwise(0))
    .withColumn("is_voice",
        when(col("call_type").isin("VOICE","IDD VOICE","VIDEO CALL"), 1).otherwise(0))
    .withColumn("is_international",
        when(col("call_type") == "IDD VOICE", 1).otherwise(0))
    .withColumn("time_of_day",
        when(col("call_hour").between(6,  11), "Morning")
       .when(col("call_hour").between(12, 17), "Afternoon")
       .when(col("call_hour").between(18, 22), "Evening")
       .otherwise("Night"))
    .withColumn("is_peak_hour",
        when(col("call_hour").between(8, 20), 1).otherwise(0))
    .withColumn("call_month",
        expr("date_format(call_date, 'yyyy-MM')"))
    .withColumn("call_week",
        expr("date_format(call_date, 'yyyy-WW')"))

    # ── Remove duplicates (keep first occurrence by call_start) ──
    .dropDuplicates(["cdr_id"])

    # ── Remove bad rows ───────────────────────────────────────────
    .filter(col("customer_id").isNotNull())
    .filter(col("call_date").isNotNull())
    .filter(col("revenue_qar") >= 0)

    # ── Audit columns ─────────────────────────────────────────────
    .withColumn("silver_load_dttm", current_timestamp())
    .withColumn("source_system",    lit("CDR_BATCH"))
)

silver_count = df_silver.count()
print(f"Silver rows: {silver_count:,} (dropped {raw_count - silver_count:,} bad rows)")

# ── CELL 6: Write Silver as Delta ─────────────────────────────────────
(df_silver.write
    .format("delta")
    .mode("overwrite")                          # use "append" for streaming
    .partitionBy("call_month")                  # partition by month for efficient queries
    .option("overwriteSchema", "true")
    .save(SILVER_CDR_PATH)
)

print(f"Silver CDRs written to: {SILVER_CDR_PATH}")

# Optimise Delta table
spark.sql(f"""
    OPTIMIZE delta.`{SILVER_CDR_PATH}`
    ZORDER BY (customer_id, call_date)
""")
print("Delta OPTIMIZE + ZORDER complete")

# ── CELL 7: Quick Validation ──────────────────────────────────────────
from pyspark.sql.functions import count, sum as spark_sum, avg, min, max, countDistinct

df_check = spark.read.format("delta").load(SILVER_CDR_PATH)

print("=== SILVER VALIDATION ===")
df_check.groupBy("call_type").agg(
    count("*").alias("cdr_count"),
    spark_sum("revenue_qar").alias("total_revenue"),
    avg("duration_minutes").alias("avg_duration_min")
).orderBy("cdr_count", ascending=False).show()

print(f"\nUnique customers: {df_check.select('customer_id').distinct().count():,}")
print(f"Date range: {df_check.agg(min('call_date')).collect()[0][0]} → {df_check.agg(max('call_date')).collect()[0][0]}")
print(f"Total revenue: QAR {df_check.agg(spark_sum('revenue_qar')).collect()[0][0]:,.2f}")

# Signal success back to ADF
dbutils.notebook.exit("SUCCESS")
