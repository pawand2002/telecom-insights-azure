# Databricks Notebook — 02_Bronze_to_Silver_Customers_Recharges
# TelecomInsights — Customer + Recharge Silver Layer
# =====================================================================

# ── CELL 1: Config ────────────────────────────────────────────────────
STORAGE_ACCOUNT = "telecominsights"   # ← change this

storage_key = dbutils.secrets.get(scope="telecom-kv", key="adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    storage_key
)

BRONZE_CUST_PATH    = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/customers/"
BRONZE_RECH_PATH    = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/recharges/"
SILVER_CUST_PATH    = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/customers/"
SILVER_RECH_PATH    = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/recharges/"

print("Paths configured")

# ── CELL 2: Read + Transform Customers ────────────────────────────────
from pyspark.sql.functions import (
    col, to_date, trim, upper, when, lit, current_timestamp,
    datediff, months_between, floor, coalesce
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

customer_schema = StructType([
    StructField("customer_id",       StringType(),  False),
    StructField("msisdn",            StringType(),  True),
    StructField("first_name",        StringType(),  True),
    StructField("last_name",         StringType(),  True),
    StructField("age",               IntegerType(), True),
    StructField("nationality",       StringType(),  True),
    StructField("language",          StringType(),  True),
    StructField("region",            StringType(),  True),
    StructField("device_type",       StringType(),  True),
    StructField("plan_type",         StringType(),  True),
    StructField("plan_name",         StringType(),  True),
    StructField("monthly_fee_qar",   DoubleType(),  True),
    StructField("data_allowance_gb", IntegerType(), True),
    StructField("activation_date",   StringType(),  True),
    StructField("tenure_months",     IntegerType(), True),
    StructField("segment",           StringType(),  True),
    StructField("churn_risk",        StringType(),  True),
    StructField("is_active",         IntegerType(), True),
    StructField("created_at",        StringType(),  True),
    StructField("updated_at",        StringType(),  True),
])

df_cust_raw = (spark.read
    .option("header", "true")
    .schema(customer_schema)
    .csv(BRONZE_CUST_PATH)
)
print(f"Bronze customers: {df_cust_raw.count():,}")

df_customers_silver = (df_cust_raw
    # Type casting
    .withColumn("activation_date", to_date(col("activation_date"), "yyyy-MM-dd"))
    # Standardise strings
    .withColumn("plan_type",   upper(trim(col("plan_type"))))
    .withColumn("plan_name",   trim(col("plan_name")))
    .withColumn("region",      trim(col("region")))
    .withColumn("segment",     trim(col("segment")))
    .withColumn("churn_risk",  trim(col("churn_risk")))
    .withColumn("nationality", trim(col("nationality")))
    # Derived features
    .withColumn("age_band",
        when(col("age") < 25, "18-24")
       .when(col("age") < 35, "25-34")
       .when(col("age") < 45, "35-44")
       .when(col("age") < 55, "45-54")
       .otherwise("55+"))
    .withColumn("tenure_band",
        when(col("tenure_months") < 6,  "0-6m")
       .when(col("tenure_months") < 12, "6-12m")
       .when(col("tenure_months") < 24, "1-2yr")
       .when(col("tenure_months") < 36, "2-3yr")
       .otherwise("3yr+"))
    .withColumn("is_high_value",
        when(col("segment").isin("High Value","VIP"), 1).otherwise(0))
    .withColumn("is_postpaid",
        when(col("plan_type") == "POSTPAID", 1).otherwise(0))
    # Null handling
    .withColumn("monthly_fee_qar",
        coalesce(col("monthly_fee_qar"), lit(0.0)))
    # Filter active customers only for Silver
    .filter(col("is_active") == 1)
    # Drop PII for Silver layer (mask MSISDN)
    # Note: in production use Unity Catalog column masking instead
    .withColumn("msisdn_masked",
        col("msisdn").substr(1, 7).concat(lit("****")))
    # Audit
    .withColumn("silver_load_dttm", current_timestamp())
    .withColumn("source_system", lit("CUSTOMER_DB"))
    # Remove duplicates
    .dropDuplicates(["customer_id"])
)

print(f"Silver customers: {df_customers_silver.count():,}")
display(df_customers_silver.limit(3))

# Write Silver customers as Delta
(df_customers_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(SILVER_CUST_PATH)
)
print(f"Silver customers written: {SILVER_CUST_PATH}")

# ── CELL 3: Read + Transform Recharges ───────────────────────────────
from pyspark.sql.types import DecimalType

recharge_schema = StructType([
    StructField("recharge_id",       StringType(),  False),
    StructField("customer_id",       StringType(),  False),
    StructField("msisdn",            StringType(),  True),
    StructField("recharge_amount",   DoubleType(),  True),
    StructField("recharge_channel",  StringType(),  True),
    StructField("recharge_dttm",     StringType(),  True),
    StructField("validity_days",     IntegerType(), True),
    StructField("expiry_dttm",       StringType(),  True),
    StructField("plan_name",         StringType(),  True),
    StructField("segment",           StringType(),  True),
    StructField("region",            StringType(),  True),
    StructField("churn_risk",        StringType(),  True),
    StructField("recharge_month",    StringType(),  True),
    StructField("recharge_date",     StringType(),  True),
    StructField("created_at",        StringType(),  True),
])

from pyspark.sql.functions import to_timestamp

df_rech_raw = (spark.read
    .option("header", "true")
    .schema(recharge_schema)
    .csv(BRONZE_RECH_PATH)
)
print(f"\nBronze recharges: {df_rech_raw.count():,}")

df_recharges_silver = (df_rech_raw
    .withColumn("recharge_dttm",
        to_timestamp(col("recharge_dttm"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("expiry_dttm",
        to_timestamp(col("expiry_dttm"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("recharge_date",
        to_date(col("recharge_date"), "yyyy-MM-dd"))
    .withColumn("recharge_amount",
        col("recharge_amount").cast(DecimalType(10,2)))
    .withColumn("recharge_channel", trim(col("recharge_channel")))
    .withColumn("segment",          trim(col("segment")))
    .withColumn("is_digital_channel",
        when(col("recharge_channel").isin("Mobile App","Web Portal","IVR"), 1)
       .otherwise(0))
    .withColumn("amount_band",
        when(col("recharge_amount") <= 25, "Low (≤25)")
       .when(col("recharge_amount") <= 75, "Mid (26-75)")
       .otherwise("High (>75)"))
    .dropDuplicates(["recharge_id"])
    .filter(col("customer_id").isNotNull())
    .filter(col("recharge_amount") > 0)
    .withColumn("silver_load_dttm", current_timestamp())
    .withColumn("source_system", lit("RECHARGE_BATCH"))
)

print(f"Silver recharges: {df_recharges_silver.count():,}")

# Write Silver recharges as Delta partitioned by month
(df_recharges_silver.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("recharge_month")
    .option("overwriteSchema", "true")
    .save(SILVER_RECH_PATH)
)
print(f"Silver recharges written: {SILVER_RECH_PATH}")

# ── CELL 4: Validate ──────────────────────────────────────────────────
from pyspark.sql.functions import count, sum as spark_sum, avg

print("\n=== CUSTOMER SILVER VALIDATION ===")
spark.read.format("delta").load(SILVER_CUST_PATH).groupBy("segment","plan_type").agg(
    count("*").alias("customers"),
    avg("monthly_fee_qar").alias("avg_fee")
).orderBy("segment").show()

print("\n=== RECHARGE SILVER VALIDATION ===")
spark.read.format("delta").load(SILVER_RECH_PATH).groupBy("recharge_channel").agg(
    count("*").alias("recharges"),
    spark_sum("recharge_amount").alias("total_amount")
).orderBy("total_amount", ascending=False).show()

dbutils.notebook.exit("SUCCESS")
