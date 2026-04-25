# TelecomInsights — Azure Lakehouse Analytics Platform

![Azure](https://img.shields.io/badge/Azure-Data%20Platform-0078D4?style=flat&logo=microsoft-azure&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Delta%20Lake-FF3621?style=flat&logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.4-E25A1C?style=flat&logo=apache-spark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=flat&logo=python&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=flat&logo=power-bi&logoColor=black)
![MLflow](https://img.shields.io/badge/MLflow-Tracking-0194E2?style=flat&logo=mlflow&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat)

---

## Overview

**TelecomInsights** is a production-grade, end-to-end Telecom Analytics Platform
built entirely on Microsoft Azure. It demonstrates real-world data engineering
across batch ingestion, real-time streaming, machine learning churn prediction,
and executive BI reporting — using the Medallion (Bronze → Silver → Gold)
Lakehouse architecture.

**Domain:** Telecommunications — Qatar market  
**Scale:** 1,000 customers · 500,000+ CDRs · 90 days · Real-time Event Hub streaming  
**Target:** Azure Data Architect portfolio project

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                │
│  CDR Files (daily CSV)  │  Customer DB (Azure SQL)  │  Network Events│
│                         │  Recharge Events           │  (real-time)  │
└────────────┬────────────┴──────────┬─────────────────┴───────┬──────┘
             │                       │                          │
             ▼                       ▼                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         INGESTION LAYER                             │
│   Azure Data Factory (batch)        Azure Event Hub (streaming)     │
│   • Tumbling Window trigger          • 32 partitions · 2 TUs        │
│   • Watermark incremental load       • 7-day retention              │
│   • Self-Hosted IR for on-prem       • Capture → ADLS Bronze        │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│               ADLS Gen2 — MEDALLION ARCHITECTURE                    │
│                                                                     │
│   Bronze (raw, append-only)                                         │
│   ├── cdrs/cdrs_YYYY_MM_DD.csv      (90 daily files)                │
│   ├── customers/customers.csv       (1,000 profiles)                │
│   ├── recharges/recharges_YYYY_MM.csv                               │
│   └── events/                      (Event Hub Capture — Avro)       │
│                                                                     │
│   Silver (cleansed · enriched · Delta Lake · partitioned)           │
│   ├── cdrs/          15 derived features · partitioned by month     │
│   ├── customers/     PII masked · tenure band · segment encoding    │
│   ├── recharges/     digital channel flag · amount band             │
│   └── events/        streaming enrichment · watermarked             │
│                                                                     │
│   Gold (KPIs · Star schema · BI-ready)                              │
│   ├── arpu_monthly/       ARPU per customer per month               │
│   ├── customer_summary/   Customer 360 — all-time metrics           │
│   ├── regional_kpis/      Revenue · drop rate · data by region      │
│   ├── daily_trend/        90-day call volume and revenue            │
│   ├── churn_scores/       ML churn probability per customer         │
│   └── realtime_alerts/    High-risk events from streaming           │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      PROCESSING LAYER                               │
│                    Azure Databricks                                 │
│                                                                     │
│  Notebook 01 — Bronze → Silver CDR                                  │
│  • Schema validation · type casting · null handling                 │
│  • 15 derived features: is_dropped, time_of_day, duration_min...    │
│  • Delta write partitioned by call_month + ZORDER by customer_id    │
│                                                                     │
│  Notebook 02 — Bronze → Silver Customers & Recharges                │
│  • MSISDN masking · age band · tenure band · segment encoding       │
│  • Digital channel flag · recharge amount band                      │
│                                                                     │
│  Notebook 03 — Silver → Gold KPIs                                   │
│  • ARPU = CDR revenue + recharge revenue + monthly plan fee         │
│  • Regional KPIs · daily trend · customer 360 summary               │
│  • Register tables in Unity Catalog for Synapse access              │
│                                                                     │
│  Notebook 04 — Churn ML Model (MLflow)                              │
│  • 19 features · RandomForest (100 trees · depth 8)                 │
│  • AUC 0.84 · Accuracy 0.81 · F1 0.79                               │
│  • Batch score all 1,000 customers → churn_probability              │
│  • MLflow experiment tracking · model registry                      │
│                                                                     │
│  Notebook 05 — Streaming Events (always-on)                         │
│  • Event Hub → Structured Streaming → Silver Delta (30-sec trigger) │
│  • foreachBatch: high-risk churn events → Gold realtime_alerts      │
│  • ADLS checkpoint · watermark 10 min · dead letter pattern         │
└─────────────────────────┬───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        SERVING LAYER                                │
│                                                                     │
│  Azure Synapse Serverless SQL Pool                                  │
│  • External tables over Gold Delta Lake                             │
│  • 5 Power BI optimised views                                       │
│                                                                     │
│  Power BI — 4-Page Executive Dashboard                              │
│  • Page 1: Executive Overview  (KPIs · Revenue · Churn · Network)   │
│  • Page 2: ARPU Analysis       (by segment · region · plan · trend) │
│  • Page 3: Churn Intelligence  (heatmap · scatter · top-20 table)   │
│  • Page 4: Network Operations  (combo chart · drop rate · 5G trend) │
│                                                                     │
│  Cosmos DB                                                          │
│  • Real-time churn alerts API for CRM team                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## KPIs Delivered

| KPI | Layer | Description |
|-----|-------|-------------|
| ARPU | Gold | Average Revenue Per User per month (QAR) |
| Churn Probability | ML | RandomForest score 0.0–1.0 per customer |
| Call Drop Rate | Silver | Dropped calls / total calls % |
| Data Consumption | Silver | MB per session, GB per customer/month |
| Recharge Frequency | Gold | Top-ups per customer per month |
| 5G Adoption | Gold | % of total calls on 5G network |
| Revenue at Risk | Gold | Monthly ARPU × high-churn customers |
| Real-time Alerts | Streaming | Churn score > 0.7 → Cosmos DB |

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Ingestion (batch) | Azure Data Factory | CDR + customer pipeline orchestration |
| Ingestion (stream) | Azure Event Hub | Real-time network event ingestion |
| Storage | ADLS Gen2 | Data lake — Bronze / Silver / Gold |
| Processing | Azure Databricks | PySpark transformation + ML |
| Storage format | Delta Lake | ACID transactions + time travel + Z-ordering |
| ML tracking | MLflow | Experiment tracking + model registry |
| Serving | Azure Synapse Analytics | Serverless SQL — external tables over Gold |
| Visualisation | Power BI | 4-page executive dashboard |
| Governance | Unity Catalog | MSISDN masking + data lineage |
| Secrets | Azure Key Vault | Zero hardcoded credentials |
| Monitoring | Azure Monitor | Pipeline alerts + cost anomaly detection |

---

## ML Model Performance

| Metric | Value |
|--------|-------|
| Algorithm | RandomForest Classifier |
| Trees / Max Depth | 100 trees · depth 8 |
| AUC (ROC) | **0.84** |
| Accuracy | 0.81 |
| F1 Score | 0.79 |
| Top Feature | tenure_months |
| High Risk Customers | 180 (18% of base) |
| Monthly Revenue at Risk | QAR 52,000 |

---

## Project Structure

```
telecom-insights-azure/
│
├── data_generation/
│   ├── generate_customers.py       Synthetic 1,000 customer profiles
│   ├── generate_cdrs.py            500K+ CDR records across 90 days
│   ├── generate_recharges.py       15K prepaid recharge events
│   ├── simulate_stream.py          Event Hub real-time simulator
│   └── run_all.py                  Master runner — generates all data
│
├── notebooks/                      Databricks notebooks (cell format)
│   ├── 01_Bronze_to_Silver_CDR.py
│   ├── 02_Bronze_to_Silver_Customers_Recharges.py
│   ├── 03_Silver_to_Gold_KPIs.py
│   ├── 04_Churn_ML_Model.py
│   └── 05_Streaming_Events.py
│
├── sql/
│   └── 01_create_tables.sql        Azure SQL source system setup
│
├── infrastructure/
│   ├── setup_azure.sh              One-command Azure resource provisioning
│   └── load_customers_to_sql.py    Seed Azure SQL with customer data
│
├── synapse/
│   └── gold_tables_and_views.sql   Synapse external tables + Power BI views
│
├── powerbi/
│   └── TelecomInsights_Dashboards.pbix
│
├── docs/
│   ├── architecture.png
│   └── screenshots/
│
└── README.md
```

---

## How to Run

### Prerequisites
- Azure subscription (free trial works)
- Python 3.8+
- Azure CLI
- Databricks workspace (Community Edition for dev)
- Power BI Desktop (free)

### Step 1 — Generate synthetic data
```bash
cd data_generation
pip install azure-eventhub
python run_all.py
```

### Step 2 — Provision Azure infrastructure
```bash
# Edit variables at top of script first
bash infrastructure/setup_azure.sh
```

### Step 3 — Load customers to Azure SQL
```bash
python infrastructure/load_customers_to_sql.py
```

### Step 4 — Trigger ADF pipelines
In Azure Portal → Data Factory → trigger all 3 pipelines manually for first run.

### Step 5 — Run Databricks notebooks (in order)
Import `.py` files from `notebooks/` into Databricks workspace.
Run: `01 → 02 → 03 → 04` then start `05` as always-on streaming job.

### Step 6 — Set up Synapse + Power BI
Run `synapse/gold_tables_and_views.sql` in Synapse Studio,
then open `powerbi/TelecomInsights_Dashboards.pbix` in Power BI Desktop.

### Step 7 — Start real-time streaming
```bash
export EVENT_HUB_CONN_STR="your_connection_string"
python data_generation/simulate_stream.py
```

---

## Key Design Decisions

**Why Medallion over a traditional DWH?**
When the ARPU calculation formula changed mid-project, we reprocessed Silver → Gold
in 20 minutes without re-extracting from source systems. In a traditional DWH
that would have required a full re-extract of 90 days of CDR data.

**Why Tumbling Window trigger over Schedule?**
Tumbling Window is stateful — if the nightly run fails, the next window waits
rather than skipping. This prevents silent data gaps in time-series KPIs like
daily ARPU and call drop rate.

**Why fixed cluster for streaming?**
Autoscaling interrupts Spark Structured Streaming micro-batches causing lag spikes.
A fixed right-sized cluster with Reserved Instance pricing gives both reliability
and 40% cost reduction versus pay-as-you-go autoscaling.

**Why Delta Lake over plain Parquet?**
ACID transactions eliminated partial write corruption during ADF pipeline failures.
MERGE for CDC, time travel for rollback after bad runs, Z-ordering for 15x query
speedup on customer + date filters.

**Why Unity Catalog for MSISDN masking?**
Column-level masking enforced at the catalog layer — analytics users see masked
MSISDN values with zero application-level changes. Audit logs capture every
access for regulatory compliance.

---

## Author

**Pawan Dubey**  
Senior Data Engineer → Azure & GCP Data Architect  
15 years experience | NCR Atleos · Cognizant · Wipro  
📍 Doha, Qatar   
🔗 [LinkedIn](https://linkedin.com/in/your-profile) | [GitHub](https://github.com/pawand2002)

---

## License

MIT License — free to use, modify, and distribute with attribution.
