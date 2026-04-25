# TelecomInsights — Azure Lakehouse Platform

## Project Overview

End-to-end Telecom Analytics Platform built on Azure, demonstrating:
- **Batch ingestion** of CDRs and customer data via Azure Data Factory
- **Real-time streaming** of network events via Azure Event Hub
- **Medallion architecture** on ADLS Gen2 with Delta Lake
- **PySpark transformations** in Azure Databricks (Bronze → Silver → Gold)
- **ML churn prediction** with MLflow on Databricks
- **BI serving** via Synapse Dedicated SQL Pool and Power BI

## Architecture

```
Sources
├── CDR Files (daily batch CSV)          → ADF → ADLS Bronze/cdrs/
├── Customer DB (Azure SQL)              → ADF → ADLS Bronze/customers/
├── Network Events (real-time)           → Event Hub → ADLS Bronze/events/
└── Recharge Events (real-time)          → Event Hub → ADLS Bronze/events/

ADLS Gen2 — Medallion
├── Bronze  (raw, append-only)
├── Silver  (cleansed, enriched, Delta Lake)
└── Gold    (KPIs, aggregations, Star schema)

Databricks
├── 01_bronze_to_silver_cdr.py          CDR cleansing + feature engineering
├── 02_bronze_to_silver_customers.py    Customer profile enrichment
├── 03_silver_to_gold_arpu.py          ARPU + revenue aggregations
├── 04_streaming_events.py             Event Hub → Delta streaming
└── 05_churn_model.py                  ML churn prediction (MLflow)

Serving
├── Synapse Dedicated SQL Pool          Gold KPIs for Power BI
├── Power BI                           ARPU, Churn, Revenue dashboards
└── Cosmos DB                          Real-time churn alerts API
```

## KPIs Computed

| KPI | Layer | Description |
|-----|-------|-------------|
| ARPU | Gold | Avg Revenue Per User per month (QAR) |
| Churn probability | ML | 0.0–1.0 score per customer |
| Data consumption | Silver | MB used per day per customer |
| Call drop rate | Silver | Dropped calls / total calls % |
| Recharge frequency | Gold | Top-ups per customer per month |
| High-value flag | Gold | Top 20% customers by revenue |
| Real-time alerts | Streaming | Churn score > 0.7 → Cosmos DB |

## Dataset

Synthetic but realistic Telecom data (Qatar market context):
- **1,000 customers** — demographics, plans, tenure, churn risk
- **~500,000 CDRs** — 90 days of call detail records
- **~15,000 recharges** — 3 months of prepaid top-up events
- **Real-time events** — network events stream via Event Hub

## Setup Instructions

### Prerequisites
- Azure Free Trial account
- Python 3.8+
- Azure CLI installed

### Step 1 — Generate Data
```bash
cd data_generation
python run_all.py
```

### Step 2 — Set up Azure Resources

**ADLS Gen2:**
```bash
az storage account create \
    --name telecominsights \
    --resource-group telecom-rg \
    --location eastus \
    --sku Standard_LRS \
    --enable-hierarchical-namespace true

az storage container create --name bronze --account-name telecominsights
az storage container create --name silver --account-name telecominsights
az storage container create --name gold   --account-name telecominsights
```

**Upload data:**
```bash
az storage blob upload-batch \
    --destination bronze \
    --source data_generation/output/ \
    --account-name telecominsights \
    --auth-mode login
```

**Azure SQL (Customer source):**
```bash
az sql server create --name telecom-sql-server --resource-group telecom-rg \
    --location eastus --admin-user sqladmin --admin-password YourPassword123!

az sql db create --resource-group telecom-rg --server telecom-sql-server \
    --name telecom-customers --edition Basic
```

**Event Hub:**
```bash
az eventhubs namespace create --name telecom-events-ns \
    --resource-group telecom-rg --location eastus --sku Standard

az eventhubs eventhub create --name telecom-events \
    --namespace-name telecom-events-ns --resource-group telecom-rg \
    --partition-count 32 --message-retention 7
```

**Key Vault:**
```bash
az keyvault create --name telecom-kv --resource-group telecom-rg --location eastus
```

### Step 3 — Configure ADF
See: adf_pipelines/pipeline_definitions.md

### Step 4 — Run Databricks Notebooks
See: databricks/ folder (notebooks in order 01 → 05)

### Step 5 — Start Streaming
```bash
cd data_generation
export EVENT_HUB_CONN_STR="your_connection_string"
python simulate_stream.py
```

## Technologies Used

| Technology | Purpose |
|------------|---------|
| Azure Data Factory | Batch orchestration |
| Azure Event Hub | Real-time streaming ingestion |
| ADLS Gen2 | Data lake storage |
| Azure Databricks | PySpark processing + ML |
| Delta Lake | ACID storage format |
| MLflow | ML experiment tracking |
| Azure Synapse | Gold layer DWH serving |
| Power BI | Business dashboards |
| Cosmos DB | Real-time alert serving |
| Azure Key Vault | Secrets management |
| Azure Monitor | Pipeline + cost monitoring |
| Unity Catalog | Data governance + MSISDN masking |

---
Built by Pawan Dubey — Azure & GCP Data Architect
