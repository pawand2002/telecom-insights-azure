# Databricks Notebook — 04_Churn_ML_Model
# TelecomInsights — Churn Prediction with MLflow
# Trains RandomForest model, scores all customers, writes results to Gold
# =====================================================================

# ── CELL 1: Config + imports ──────────────────────────────────────────
import mlflow
import mlflow.spark
from mlflow.models.signature import infer_signature

from pyspark.ml import Pipeline
from pyspark.ml.feature import (VectorAssembler, StringIndexer,
                                 StandardScaler, Imputer)
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import (col, when, lit, current_timestamp,
                                    round as spark_round, coalesce)

STORAGE_ACCOUNT  = "telecominsights"   # ← change this
SILVER_CDR_PATH  = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/cdrs/"
SILVER_CUST_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/customers/"
SILVER_RECH_PATH = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/recharges/"
GOLD_CHURN_PATH  = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/churn_scores/"

storage_key = dbutils.secrets.get(scope="telecom-kv", key="adls-storage-key")
spark.conf.set(
    f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net",
    storage_key
)
print("Config ready")

# ── CELL 2: Build feature table ───────────────────────────────────────
# Load Silver
df_cdr  = spark.read.format("delta").load(SILVER_CDR_PATH)
df_cust = spark.read.format("delta").load(SILVER_CUST_PATH)
df_rech = spark.read.format("delta").load(SILVER_RECH_PATH)

df_cdr.createOrReplaceTempView("cdr")
df_cust.createOrReplaceTempView("customers")
df_rech.createOrReplaceTempView("recharges")

# Build feature set — one row per customer
df_features = spark.sql("""
    SELECT
        c.customer_id,
        -- Label: churn_risk High = 1, else = 0
        CASE WHEN c.churn_risk = 'High' THEN 1 ELSE 0 END AS label,

        -- Customer attributes
        c.tenure_months,
        c.monthly_fee_qar,
        c.data_allowance_gb,
        CASE WHEN c.plan_type = 'PREPAID' THEN 1 ELSE 0 END AS is_prepaid,
        CASE WHEN c.segment = 'Low Value' THEN 0
             WHEN c.segment = 'Mid Value' THEN 1
             WHEN c.segment = 'High Value' THEN 2
             ELSE 3 END AS segment_encoded,

        -- CDR features (last 90 days behaviour)
        COUNT(cdr.cdr_id)                                   AS total_calls,
        COALESCE(SUM(cdr.revenue_qar), 0)                   AS total_cdr_revenue,
        COALESCE(AVG(cdr.duration_minutes), 0)              AS avg_call_duration,
        COALESCE(SUM(cdr.data_consumed_mb)/1024, 0)         AS total_data_gb,
        COALESCE(SUM(cdr.is_dropped), 0)                    AS dropped_calls,
        COALESCE(
            SUM(cdr.is_dropped)*100.0/NULLIF(COUNT(cdr.cdr_id),0), 0
        )                                                   AS drop_rate_pct,
        COUNT(DISTINCT cdr.call_date)                       AS active_days,
        COALESCE(SUM(cdr.is_roaming), 0)                    AS roaming_calls,
        COALESCE(
            SUM(CASE WHEN cdr.network_type = '5G' THEN 1 END), 0
        )                                                   AS calls_5g,
        COALESCE(AVG(cdr.is_peak_hour), 0)                  AS peak_hour_ratio,

        -- Recharge features
        COALESCE(COUNT(DISTINCT r.recharge_id), 0)          AS total_recharges,
        COALESCE(SUM(r.recharge_amount), 0)                 AS total_recharge_amount,
        COALESCE(AVG(r.recharge_amount), 0)                 AS avg_recharge_amount,
        COALESCE(
            SUM(CASE WHEN r.is_digital_channel = 1 THEN 1 END), 0
        )                                                   AS digital_recharges

    FROM customers c
    LEFT JOIN cdr    ON c.customer_id = cdr.customer_id
    LEFT JOIN recharges r ON c.customer_id = r.customer_id
    GROUP BY
        c.customer_id, c.churn_risk, c.tenure_months,
        c.monthly_fee_qar, c.data_allowance_gb, c.plan_type,
        c.segment
""")

print(f"Feature rows: {df_features.count():,}")
print(f"Churners:     {df_features.filter(col('label')==1).count():,}")
print(f"Non-churners: {df_features.filter(col('label')==0).count():,}")

display(df_features.limit(5))

# ── CELL 3: Prepare ML pipeline ──────────────────────────────────────
feature_cols = [
    "tenure_months", "monthly_fee_qar", "data_allowance_gb",
    "is_prepaid", "segment_encoded",
    "total_calls", "total_cdr_revenue", "avg_call_duration",
    "total_data_gb", "dropped_calls", "drop_rate_pct",
    "active_days", "roaming_calls", "calls_5g", "peak_hour_ratio",
    "total_recharges", "total_recharge_amount", "avg_recharge_amount",
    "digital_recharges"
]

# Impute nulls (some customers may have no CDRs)
imputer = Imputer(
    inputCols=feature_cols,
    outputCols=[f"{c}_imp" for c in feature_cols],
    strategy="median"
)
imputed_cols = [f"{c}_imp" for c in feature_cols]

# Assemble features into a single vector
assembler = VectorAssembler(
    inputCols=imputed_cols,
    outputCol="features_raw",
    handleInvalid="skip"
)

# Scale features
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=True
)

# RandomForest classifier
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=8,
    seed=42,
    probabilityCol="probability",
    rawPredictionCol="rawPrediction"
)

# Full pipeline
pipeline = Pipeline(stages=[imputer, assembler, scaler, rf])

# Train/test split — 80/20
df_train, df_test = df_features.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {df_train.count():,} | Test: {df_test.count():,}")

# ── CELL 4: Train with MLflow tracking ───────────────────────────────
mlflow.set_experiment("/Shared/TelecomInsights/ChurnPrediction")

with mlflow.start_run(run_name="RandomForest_v1") as run:
    run_id = run.info.run_id
    print(f"MLflow Run ID: {run_id}")

    # Log parameters
    mlflow.log_params({
        "model_type":   "RandomForestClassifier",
        "num_trees":    100,
        "max_depth":    8,
        "train_rows":   df_train.count(),
        "test_rows":    df_test.count(),
        "features":     len(feature_cols),
        "label":        "churn_risk_binary"
    })

    # Train
    print("Training RandomForest model...")
    model = pipeline.fit(df_train)

    # Predict on test set
    df_predictions = model.transform(df_test)

    # Evaluate
    evaluator_auc = BinaryClassificationEvaluator(
        labelCol="label",
        rawPredictionCol="rawPrediction",
        metricName="areaUnderROC"
    )
    evaluator_acc = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="accuracy"
    )
    evaluator_f1 = MulticlassClassificationEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="f1"
    )

    auc      = evaluator_auc.evaluate(df_predictions)
    accuracy = evaluator_acc.evaluate(df_predictions)
    f1_score = evaluator_f1.evaluate(df_predictions)

    # Log metrics
    mlflow.log_metrics({
        "auc":      round(auc, 4),
        "accuracy": round(accuracy, 4),
        "f1_score": round(f1_score, 4)
    })

    print(f"\n=== MODEL PERFORMANCE ===")
    print(f"  AUC:      {auc:.4f}")
    print(f"  Accuracy: {accuracy:.4f}")
    print(f"  F1 Score: {f1_score:.4f}")

    # Log feature importance
    rf_model   = model.stages[-1]
    importances = rf_model.featureImportances.toArray()
    for feat, imp in sorted(zip(feature_cols, importances),
                            key=lambda x: x[1], reverse=True)[:10]:
        print(f"  Feature: {feat:35s} Importance: {imp:.4f}")
        mlflow.log_metric(f"feat_imp_{feat[:20]}", round(float(imp), 4))

    # Log model
    mlflow.spark.log_model(model, "churn_model")
    model_uri = f"runs:/{run_id}/churn_model"
    print(f"\nModel logged: {model_uri}")

# ── CELL 5: Score ALL customers (production inference) ───────────────
print("\nScoring all customers...")
df_all_scored = model.transform(df_features)

from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

# Extract churn probability (probability of class 1)
extract_prob = udf(lambda v: float(v[1]), DoubleType())

df_churn_scores = (df_all_scored
    .withColumn("churn_probability",
        extract_prob(col("probability")))
    .withColumn("churn_score_band",
        when(col("churn_probability") >= 0.70, "High Risk")
       .when(col("churn_probability") >= 0.40, "Medium Risk")
       .otherwise("Low Risk"))
    .withColumn("is_high_churn_risk",
        when(col("churn_probability") >= 0.70, 1).otherwise(0))
    .withColumn("model_version",    lit("rf_v1"))
    .withColumn("score_dttm",       current_timestamp())
    .withColumn("mlflow_run_id",    lit(run_id))
    .select(
        "customer_id", "churn_probability", "churn_score_band",
        "is_high_churn_risk", "label", "tenure_months",
        "monthly_fee_qar", "total_calls", "drop_rate_pct",
        "total_recharges", "active_days",
        "model_version", "score_dttm", "mlflow_run_id"
    )
)

high_risk_count = df_churn_scores.filter(col("is_high_churn_risk") == 1).count()
print(f"Scored customers:      {df_churn_scores.count():,}")
print(f"High churn risk (≥70%): {high_risk_count:,}")

# ── CELL 6: Write churn scores to Gold ───────────────────────────────
(df_churn_scores.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(GOLD_CHURN_PATH)
)
print(f"Churn scores written: {GOLD_CHURN_PATH}")

# Register in catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS telecom_gold.churn_scores
    USING DELTA LOCATION '{GOLD_CHURN_PATH}'
""")

# ── CELL 7: Score distribution ───────────────────────────────────────
print("\n=== CHURN SCORE DISTRIBUTION ===")
df_churn_scores.groupBy("churn_score_band").count() \
    .orderBy("count", ascending=False).show()

display(df_churn_scores.orderBy("churn_probability", ascending=False).limit(20))

dbutils.notebook.exit(f"SUCCESS|AUC={auc:.4f}|HighRisk={high_risk_count}")
