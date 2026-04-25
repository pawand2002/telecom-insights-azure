
---

```markdown
# TelecomInsights — Azure Lakehouse Analytics Platform

![Azure](https://img.shields.io/badge/Azure-Data%20Platform-0078D4?logo=microsoft-azure)
![Databricks](https://img.shields.io/badge/Databricks-Delta%20Lake-FF3621?logo=databricks)
![Python](https://img.shields.io/badge/Python-3.10-3776AB?logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.4-E25A1C?logo=apache-spark)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=power-bi)
![License](https://img.shields.io/badge/License-MIT-green)

## Project Overview

End-to-end **Telecom Analytics Platform** built on Microsoft Azure,
demonstrating production-grade data engineering across batch ingestion,
real-time streaming, ML churn prediction, and executive BI dashboards.

**Domain:** Telecommunications (Qatar market — CDR processing, ARPU, Churn)
**Architecture:** Azure Lakehouse with Medallion (Bronze → Silver → Gold)
**Scale:** 1,000 customers | 500,000+ CDRs | 90 days | Real-time Event Hub streaming

---

## Architecture

![Architecture Diagram](docs/architecture.png)

```
Sources
├── CDR Files (daily CSV)          → ADF → ADLS Bronze
├── Customer DB (Azure SQL)        → ADF → ADLS Bronze
├── Network Events (real-time)     → Event Hub (32 partitions) → ADLS Bronze
└── Recharge Events (real-time)    → Event Hub → ADLS Bronze

ADLS Gen2 — Medallion
├── Bronze  (raw, append-only, schema-on-read)
├── Silver  (cleansed, enriched, Delta Lake, partitioned)
└── Gold    (KPIs, aggregations, Star schema)

Databricks
├── 01_Bronze_to_Silver_CDR.py          CDR cleansing + 15 features
├── 02_Bronze_to_Silver_Customers.py    Customer enrichment + PII masking
├── 03_Silver_to_Gold_KPIs.py          ARPU + regional + daily trend
├── 04_Churn_ML_Model.py               RandomForest + MLflow (AUC 0.84)
└── 05_Streaming_Events.py             Event Hub Structured Streaming

Serving
├── Synapse Dedicated SQL Pool          Gold KPIs → Power BI
├── Power BI (4-page dashboard)         ARPU | Churn | Network | Executive
└── Cosmos DB                          Real-time churn alerts API
```

---

## KPIs Computed

| KPI | Layer | Description |
|-----|-------|-------------|
| ARPU | Gold | Average Revenue Per User per month (QAR) |
| Churn Probability | ML | RandomForest score 0.0–1.0 per customer |
| Call Drop Rate | Silver | Dropped calls / total calls % |
| Data Consumption | Silver | MB per session, GB per customer/month |
| Recharge Frequency | Gold | Top-ups per customer per month |
| 5G Adoption | Gold | % of calls on 5G network |
| Revenue at Risk | Gold | ARPU × high-churn customers |
| Real-time Alerts | Streaming | Churn score > 0.7 → Cosmos DB |

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Ingestion (batch) | Azure Data Factory | CDR + customer pipeline orchestration |
| Ingestion (stream) | Azure Event Hub | Real-time network event ingestion |
| Storage | ADLS Gen2 | Data lake — Bronze/Silver/Gold |
| Processing | Azure Databricks | PySpark transformation + ML |
| Storage format | Delta Lake | ACID transactions + time travel |
| Orchestration | ADF Tumbling Window | Stateful daily batch scheduling |
| ML tracking | MLflow | Experiment tracking + model registry |
| Serving | Azure Synapse Analytics | Gold DWH for Power BI |
| Visualisation | Power BI | 4-page executive dashboard |
| Governance | Unity Catalog | MSISDN masking + data lineage |
| Secrets | Azure Key Vault | Zero hardcoded credentials |
| Monitoring | Azure Monitor | Pipeline alerts + cost tracking |

---

## Project Structure

```
telecom-insights-azure/
├── data_generation/
│   ├── generate_customers.py     Synthetic 1,000 customer profiles
│   ├── generate_cdrs.py          500K+ CDR records (90 days)
│   ├── generate_recharges.py     15K prepaid recharge events
│   ├── simulate_stream.py        Event Hub real-time simulator
│   └── run_all.py                Master runner
├── notebooks/                    Databricks notebooks (cell format)
│   ├── 01_Bronze_to_Silver_CDR.py
│   ├── 02_Bronze_to_Silver_Customers_Recharges.py
│   ├── 03_Silver_to_Gold_KPIs.py
│   ├── 04_Churn_ML_Model.py
│   └── 05_Streaming_Events.py
├── sql/
│   └── 01_create_tables.sql      Azure SQL source system setup
├── infrastructure/
│   ├── setup_azure.sh            One-command Azure resource provisioning
│   └── load_customers_to_sql.py  Seed Azure SQL with customer data
├── synapse/
│   └── gold_tables_and_views.sql Synapse external tables + Power BI views
├── powerbi/
│   └── powerbi_complete_guide.md Dashboard build instructions
├── docs/
│   ├── architecture.png          Architecture diagram
│   └── screenshots/              Dashboard screenshots
└── README.md
```

---

## ML Model Performance

| Metric | Value |
|--------|-------|
| Model | RandomForest (100 trees, depth 8) |
| AUC | 0.84 |
| Accuracy | 0.81 |
| F1 Score | 0.79 |
| Top Feature | tenure_months |
| High Risk Customers | ~180 (18%) |
| Revenue at Risk | QAR 52,000/month |

---

## Dashboard Screenshots

### Executive Overview
![Executive Overview](docs/screenshots/page1_executive.png)

### Churn Intelligence
![Churn Analysis](docs/screenshots/page3_churn.png)

---

## How to Run This Project

### Prerequisites
- Azure subscription (free trial works)
- Python 3.8+
- Azure CLI installed
- Databricks workspace (Community Edition for dev)
- Power BI Desktop (free)

### Step 1 — Generate Synthetic Data
```bash
cd data_generation
pip install azure-eventhub
python run_all.py
```

### Step 2 — Provision Azure Infrastructure
```bash
# Edit variables at top of script first
chmod +x infrastructure/setup_azure.sh
bash infrastructure/setup_azure.sh
```

### Step 3 — Load SQL + Run ADF Pipelines
```bash
# Load customers to Azure SQL
python infrastructure/load_customers_to_sql.py

# Then trigger ADF pipelines in Azure portal
```

### Step 4 — Run Databricks Notebooks (in order)
```
01 → 02 → 03 → 04 → 05 (always-on)
```
Import .py files into Databricks workspace — they are in notebook cell format.

### Step 5 — Set Up Synapse + Power BI
Run `synapse/gold_tables_and_views.sql` in Synapse Studio,
then follow `powerbi/powerbi_complete_guide.md`.

---

## Key Design Decisions

**Why Medallion over single-layer DWH?**
Each layer is independently queryable and reprocessable. When business logic changed for ARPU calculation, we reprocessed Silver → Gold without re-extracting from source systems.

**Why Delta Lake over plain Parquet?**
ACID transactions eliminated partial write corruption during ADF failures. MERGE for CDC, time travel for audit, Z-ordering for query performance.

**Why Tumbling Window trigger over Schedule?**
Stateful — if nightly run fails, next window waits rather than skipping. Prevents silent data gaps in time-series KPIs.

**Why fixed cluster for streaming?**
Autoscaling interrupts Spark Structured Streaming micro-batches causing lag spikes. Fixed right-sized cluster with Reserved Instance pricing gives both reliability and cost control.

---

## Author

**Pawan Dubey**
Senior Data Engineer → Azure & GCP Data Architect
15 years experience | NCR Atleos | Cognizant | Wipro
📍 Doha, Qatar | Open to relocation (India, Pune preferred)
🔗 [LinkedIn](https://linkedin.com/in/your-profile)

---

## License
MIT License — free to use, modify, and distribute with attribution.
```

---
