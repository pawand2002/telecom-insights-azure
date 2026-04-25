# Databricks Notebook — 05_Streaming_Events
# TelecomInsights — Event Hub → Structured Streaming → Silver Delta
# Run as a long-running cluster job (not scheduled — always on)
# =====================================================================

# ── CELL 1: Config ────────────────────────────────────────────────────
STORAGE_ACCOUNT  = "telecominsights"   # ← change this

storage_key = dbutils.secrets.get(scope="telecom-kv", key="adls-storage-key")
eh_conn_str = dbutils.secrets.get(scope="telecom-kv", key="eventhub-connection-string")

spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    storage_key
)

SILVER_EVENTS_PATH   = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/events/"
CHECKPOINT_PATH      = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/_checkpoints/events/"
GOLD_REALTIME_PATH   = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/realtime_alerts/"

# ── CELL 2: Event Hub connection config ──────────────────────────────
# Event Hub connection string format for Spark connector
EH_NAMESPACE   = "telecom-events-ns"       # ← change this
EH_NAME        = "telecom-events"

eventhub_conf = {
    "eventhubs.connectionString":
        sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eh_conn_str),
    "eventhubs.consumerGroup":         "$Default",
    "eventhubs.startingPosition":      '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    "eventhubs.maxEventsPerTrigger":   1000,
}

print("Event Hub config ready")
print(f"Reading from: {EH_NAMESPACE}/{EH_NAME}")
print(f"Silver path:  {SILVER_EVENTS_PATH}")

# ── CELL 3: Define message schema ─────────────────────────────────────
from pyspark.sql.types import (StructType, StructField, StringType,
                                DoubleType, IntegerType, TimestampType)

event_schema = StructType([
    StructField("event_id",          StringType(),  False),
    StructField("event_type",        StringType(),  True),
    StructField("customer_id",       StringType(),  True),
    StructField("msisdn",            StringType(),  True),
    StructField("event_time",        StringType(),  True),
    StructField("network_type",      StringType(),  True),
    StructField("tower_id",          StringType(),  True),
    StructField("region",            StringType(),  True),
    StructField("segment",           StringType(),  True),
    StructField("plan_name",         StringType(),  True),
    StructField("churn_risk",        StringType(),  True),
    StructField("duration_seconds",  IntegerType(), True),
    StructField("data_mb",           DoubleType(),  True),
    StructField("call_type",         StringType(),  True),
    StructField("signal_strength",   IntegerType(), True),
    StructField("recharge_amount",   DoubleType(),  True),
    StructField("recharge_channel",  StringType(),  True),
    StructField("roaming_country",   StringType(),  True),
])

# ── CELL 4: Read stream from Event Hub ───────────────────────────────
from pyspark.sql.functions import (col, from_json, current_timestamp,
                                    to_timestamp, when, lit, coalesce)

df_raw_stream = (spark
    .readStream
    .format("eventhubs")
    .options(**eventhub_conf)
    .load()
)

# Event Hub delivers body as binary — cast to string then parse JSON
df_parsed = (df_raw_stream
    .withColumn("body",  col("body").cast("string"))
    .withColumn("data",  from_json(col("body"), event_schema))
    .select(
        "data.*",
        col("enqueuedTime").alias("enqueued_time"),
        col("partition").cast("string").alias("eh_partition"),
        col("offset").alias("eh_offset")
    )
)

# ── CELL 5: Silver enrichment ─────────────────────────────────────────
df_silver_stream = (df_parsed
    .withColumn("event_time",
        to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .withColumn("data_mb",
        coalesce(col("data_mb"), lit(0.0)))
    .withColumn("duration_seconds",
        coalesce(col("duration_seconds"), lit(0)))
    .withColumn("is_dropped",
        when(col("event_type") == "call_dropped", 1).otherwise(0))
    .withColumn("is_recharge",
        when(col("event_type") == "recharge_completed", 1).otherwise(0))
    .withColumn("is_data_event",
        when(col("event_type").isin(
            "data_session_start","data_session_end"), 1).otherwise(0))
    .withColumn("is_high_churn_customer",
        when(col("churn_risk") == "High", 1).otherwise(0))
    .withColumn("silver_load_dttm", current_timestamp())
    .filter(col("event_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    # Add watermark for late data handling
    .withWatermark("event_time", "10 minutes")
)

# ── CELL 6: Write Silver stream to Delta ─────────────────────────────
silver_query = (df_silver_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start(SILVER_EVENTS_PATH)
)

print(f"Silver streaming started → {SILVER_EVENTS_PATH}")
print(f"Checkpoint: {CHECKPOINT_PATH}")
print("Trigger: every 30 seconds")

# ── CELL 7: Real-time churn alert — foreachBatch pattern ─────────────
# Write high-risk churn customers to Gold realtime alerts table
# This runs in parallel with the Silver write above

def process_churn_alerts(df_batch, batch_id):
    """Called for each micro-batch — write high-risk customers to Gold"""
    if df_batch.isEmpty():
        return

    df_alerts = (df_batch
        .filter(col("churn_risk") == "High")
        .filter(col("event_type").isin(
            "call_dropped", "data_session_end", "recharge_completed"))
        .select(
            "customer_id", "msisdn", "event_type", "event_time",
            "region", "segment", "plan_name", "churn_risk",
            "enqueued_time"
        )
        .withColumn("alert_type",    lit("CHURN_RISK_ACTIVITY"))
        .withColumn("batch_id",      lit(batch_id))
        .withColumn("alert_created", current_timestamp())
    )

    alert_count = df_alerts.count()
    if alert_count > 0:
        # Write to Gold realtime alerts
        (df_alerts.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(GOLD_REALTIME_PATH)
        )
        print(f"Batch {batch_id}: {alert_count} churn alerts written")


alert_query = (df_silver_stream
    .writeStream
    .foreachBatch(process_churn_alerts)
    .option("checkpointLocation", f"{CHECKPOINT_PATH}_alerts/")
    .trigger(processingTime="30 seconds")
    .start()
)

print("\nBoth streaming queries running:")
print("  1. Silver events Delta write")
print("  2. Real-time churn alerts (foreachBatch)")
print("\nTo monitor: spark.streams.active")
print("To stop:    silver_query.stop() | alert_query.stop()")

# ── CELL 8: Monitor streaming (run manually to check status) ─────────
# Uncomment and run this cell separately to check streaming progress
# import time
# for i in range(10):
#     print(f"\n--- Check {i+1} ---")
#     for q in spark.streams.active:
#         progress = q.lastProgress
#         if progress:
#             print(f"  Query: {q.name or q.id}")
#             print(f"  Input rows/sec: {progress.get('inputRowsPerSecond', 0):.1f}")
#             print(f"  Processing time: {progress.get('batchDuration', 0)}ms")
#     time.sleep(30)
