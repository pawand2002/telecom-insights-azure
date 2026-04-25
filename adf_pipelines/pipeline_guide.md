# TelecomInsights — ADF Pipeline Guide
# Step-by-step setup for all 3 ADF pipelines

## Overview — 3 Pipelines to Build

| Pipeline | Type | Source | Sink | Trigger |
|----------|------|--------|------|---------|
| PL_Customers_FullLoad | Full reload | Azure SQL | ADLS Bronze/customers/ | Daily 1 AM |
| PL_CDR_IncrementalLoad | Incremental | ADLS Bronze/cdrs/ | ADLS Silver/cdrs/ | Daily 3 AM |
| PL_Recharge_Incremental | Incremental | ADLS Bronze/recharges/ | ADLS Silver/recharges/ | Daily 3 AM |

---

## Step 1 — Create Linked Services

### 1A. Azure SQL Linked Service
In ADF Studio → Manage → Linked Services → New

```
Name:              LS_AzureSQL_Telecom
Type:              Azure SQL Database
Authentication:    SQL Authentication
Server:            your-server.database.windows.net
Database:          telecom-customers
Username:          sqladmin
Password:          [Key Vault reference → sql-password]
```

Test connection → Create

### 1B. ADLS Gen2 Linked Service
```
Name:              LS_ADLS_TelecomInsights
Type:              Azure Data Lake Storage Gen2
Authentication:    Account Key
Account:           your-storage-account
Key:               [Key Vault reference → adls-storage-key]
```

Test connection → Create

### 1C. Key Vault Linked Service (create this FIRST)
```
Name:              LS_KeyVault_Telecom
Type:              Azure Key Vault
URL:               https://your-keyvault.vault.azure.net/
```

Note: ADF managed identity must have Key Vault Secrets User role
(setup_azure.sh does this automatically)

---

## Pipeline 1 — PL_Customers_FullLoad

### Purpose
Full reload of customer profiles from Azure SQL to ADLS Bronze daily.
Customers are a small table (1000 rows) — full reload is simpler than CDC.

### Activities
```
[Lookup: Get Row Count] → [Copy: SQL to ADLS] → [SP: Update Watermark] → [On Failure: Web Alert]
```

### Activity 1 — Copy Activity
```
Name:            ACT_Copy_Customers
Type:            Copy Activity

Source:
  Linked Service:   LS_AzureSQL_Telecom
  Query:            SELECT * FROM dbo.customers WHERE is_active = 1

Sink:
  Linked Service:   LS_ADLS_TelecomInsights
  File path:        bronze / customers / customers.csv
  File format:      DelimitedText (CSV)
  Column delimiter: ,
  Row delimiter:    \n
  First row header: true
  Write behavior:   Overwrite

Settings:
  Degree of copy parallelism: 4
  Fault tolerance:            Skip incompatible rows
  Log incompatible rows:      true → bronze/logs/customers_errors.csv
```

### Activity 2 — Stored Procedure (update watermark)
```
Name:            ACT_Update_Watermark_Customers
Type:            Stored Procedure
Runs after:      ACT_Copy_Customers (Success)

Linked Service:  LS_AzureSQL_Telecom
Procedure:       dbo.usp_update_watermark
Parameters:
  @pipeline_name:  customers_full_load
  @new_watermark:  @{utcNow()}
  @status:         SUCCESS
  @rows_written:   @{activity('ACT_Copy_Customers').output.rowsWritten}
```

### Activity 3 — Web Activity (Teams alert on failure)
```
Name:            ACT_Alert_On_Failure
Type:            Web Activity
Runs after:      ACT_Copy_Customers (Failure)

URL:             https://webhook.site/your-unique-id  (use webhook.site for testing)
Method:          POST
Body:            {
                   "pipeline": "PL_Customers_FullLoad",
                   "status": "FAILED",
                   "error": "@{activity('ACT_Copy_Customers').error.message}",
                   "time": "@{utcNow()}"
                 }
```

### Trigger
```
Name:            TR_Customers_Daily
Type:            Schedule
Frequency:       Daily
Time:            01:00 AM UTC
Start:           today
```

---

## Pipeline 2 — PL_CDR_IncrementalLoad

### Purpose
Incremental load of CDR files from Bronze to Silver.
Uses filename-based partitioning — each file is one day.
Watermark tracks last processed date to avoid reprocessing.

### Activities
```
[Lookup: Get Watermark] → [Get Metadata: List Files] → [Filter: New Files Only]
→ [ForEach: Process Each File]
    → [Copy: Bronze to Silver]
    → [SP: Update Watermark]
→ [On Failure: Web Alert]
```

### Activity 1 — Lookup (get last watermark)
```
Name:            ACT_Lookup_CDR_Watermark
Type:            Lookup
Linked Service:  LS_AzureSQL_Telecom
Query:           SELECT last_watermark FROM dbo.pipeline_watermark
                 WHERE pipeline_name = 'cdrs_incremental_load'
First row only:  true
```

### Activity 2 — Get Metadata (list CDR files in Bronze)
```
Name:            ACT_GetMetadata_CDR_Files
Type:            Get Metadata
Linked Service:  LS_ADLS_TelecomInsights
File path:       bronze / cdrs
Field list:      childItems
```

### Activity 3 — Filter (only files newer than watermark)
```
Name:            ACT_Filter_New_CDR_Files
Type:            Filter
Items:           @activity('ACT_GetMetadata_CDR_Files').output.childItems
Condition:       @greater(
                   formatDateTime(
                     replace(replace(item().name,'cdrs_',''),'.csv',''),
                     'yyyy_MM_dd'
                   ),
                   activity('ACT_Lookup_CDR_Watermark').output.firstRow.last_watermark
                 )
```

### Activity 4 — ForEach (process each new file)
```
Name:            ACT_ForEach_CDR_Files
Type:            ForEach
Items:           @activity('ACT_Filter_New_CDR_Files').output.Value
Batch count:     4     (parallel processing of 4 files at once)
Is sequential:   false
```

### Inside ForEach — Copy Activity
```
Name:            ACT_Copy_CDR_Bronze_to_Silver
Type:            Copy Activity

Source:
  Linked Service: LS_ADLS_TelecomInsights
  File path:      bronze / cdrs / @{item().name}
  Format:         DelimitedText (CSV, header=true)

Sink:
  Linked Service: LS_ADLS_TelecomInsights
  File path:      silver / cdrs / @{substring(item().name,5,10)} / @{item().name}
                  (creates silver/cdrs/2024-10-01/cdrs_2024_10_01.csv)
  Format:         Parquet  ← convert CSV to Parquet in Silver
  Compression:    snappy

Column Mapping (explicit — never trust auto-map in production):
  cdr_id           → cdr_id
  customer_id      → customer_id
  msisdn           → msisdn
  call_type        → call_type
  call_start_dttm  → call_start_dttm
  call_end_dttm    → call_end_dttm
  duration_seconds → duration_seconds
  network_type     → network_type
  data_consumed_mb → data_consumed_mb
  termination_code → termination_code
  is_roaming       → is_roaming
  revenue_qar      → revenue_qar
  call_date        → call_date
```

### Activity 5 — Stored Procedure (update watermark after ForEach)
```
Name:            ACT_Update_CDR_Watermark
Type:            Stored Procedure
Runs after:      ACT_ForEach_CDR_Files (Success)

Procedure:       dbo.usp_update_watermark
Parameters:
  @pipeline_name:  cdrs_incremental_load
  @new_watermark:  @{utcNow()}
  @status:         SUCCESS
  @rows_written:   @{activity('ACT_ForEach_CDR_Files').output.itemsCount}
```

### Trigger
```
Name:            TR_CDR_Daily
Type:            Tumbling Window   ← not Schedule — we want stateful retry
Frequency:       Daily
Interval:        1
Start:           2024-10-01 03:00:00 UTC
Max retries:     2
Retry interval:  30 minutes
```

---

## Pipeline 3 — PL_Recharge_Incremental

### Purpose
Incremental load of recharge files from Bronze to Silver.
Same pattern as CDR pipeline — filename-based partitioning by month.

### Activities (same pattern as CDR pipeline)
```
[Lookup: Get Watermark] → [Get Metadata: List Files] → [Filter: New Files]
→ [ForEach: Copy to Silver (Parquet)] → [SP: Update Watermark]
→ [On Failure: Alert]
```

Key differences from CDR pipeline:
- Source: bronze/recharges/
- Sink:   silver/recharges/YYYY_MM/
- Watermark: recharges_incremental
- Monthly files (not daily) so fewer iterations

---

## ADF Pipeline Parameters — Best Practices

### Add these parameters to all pipelines:
```
Parameter Name    Type      Default Value     Purpose
p_env             String    dev               dev/test/prod routing
p_source_folder   String    bronze            override source container
p_sink_folder     String    silver            override sink container
p_debug_mode      Bool      false             extra logging when true
```

### Dynamic content examples:
```
# Dynamic source path with environment
@concat(pipeline().parameters.p_source_folder, '/cdrs/', item().name)

# Conditional logging
@if(pipeline().parameters.p_debug_mode, 'Verbose', 'Normal')

# Current run window (Tumbling Window trigger)
@trigger().outputs.windowStartTime
@trigger().outputs.windowEndTime
```

---

## Monitoring — Key Metrics to Watch

In ADF Studio → Monitor:

1. Pipeline runs — check status of all 3 pipelines
2. Activity runs — drill into which activity failed
3. Trigger runs — verify Tumbling Window triggers fired correctly

### Azure Monitor Alerts to set up:
```
Alert 1: Pipeline Failed
  Signal:     Pipeline run succeeded = 0 (in 1 hour)
  Severity:   2 (Warning)
  Action:     Email + Teams webhook

Alert 2: Long Running Pipeline
  Signal:     Pipeline duration > 60 minutes
  Severity:   3 (Info)
  Action:     Email only

Alert 3: High Copy Activity Data Read
  Signal:     Data read > 10 GB in single run
  Severity:   3 (Info)
  Action:     Email — check for full reload instead of incremental
```

---

## Testing Your Pipelines

### Test 1 — Manual trigger (first run)
1. Go to PL_Customers_FullLoad → Debug
2. Verify: 1000 rows copied to bronze/customers/customers.csv
3. Verify: watermark table updated in Azure SQL

### Test 2 — Incremental logic
1. In Azure SQL: UPDATE a few customer records
   ```sql
   UPDATE dbo.customers SET churn_risk = 'High', updated_at = GETDATE()
   WHERE segment = 'Low Value' AND tenure_months < 3
   ```
2. Run PL_Customers_FullLoad again
3. Verify: customers.csv in ADLS updated with new values

### Test 3 — ForEach CDR pipeline
1. Run PL_CDR_IncrementalLoad → Debug
2. Watch ForEach process each daily CDR file
3. Verify: silver/cdrs/ has Parquet files with correct dates
4. Check watermark updated in Azure SQL

### Validate data in ADLS (Azure CLI):
```bash
# Count files in bronze CDRs
az storage blob list --container-name bronze \
    --account-name your-storage \
    --prefix cdrs/ \
    --query "length(@)" --auth-mode login

# Count files in silver CDRs
az storage blob list --container-name silver \
    --account-name your-storage \
    --prefix cdrs/ \
    --query "length(@)" --auth-mode login
```

---

## Common Errors & Fixes

| Error | Cause | Fix |
|-------|-------|-----|
| LinkedService connection failed | Firewall not open | Add ADF IP to SQL firewall |
| Access denied on ADLS | Managed identity not granted | Re-run role assignment in setup_azure.sh |
| Key Vault access denied | ADF not in Key Vault policy | Add ADF principal to KV access policy |
| ForEach fails on one file | Bad CSV row | Enable fault tolerance in Copy Activity |
| Watermark not updating | SP call failing | Check SQL permissions for ADF managed identity |
| Tumbling Window stuck | Previous run still running | Set max concurrent runs = 1 |
