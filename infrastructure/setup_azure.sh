#!/bin/bash
# ============================================================
# TelecomInsights — Azure Infrastructure Setup
# Run these commands in Azure Cloud Shell or local Azure CLI
# ============================================================

# ── Variables — change these to your preferences ─────────────
RESOURCE_GROUP="telecom-insights-rg"
LOCATION="eastus"
STORAGE_ACCOUNT="telecominsights$(date +%s | tail -c 5)"  # unique name
SQL_SERVER="telecom-sql-$(date +%s | tail -c 5)"
SQL_DB="telecom-customers"
SQL_ADMIN="sqladmin"
SQL_PASSWORD="TelecomP@ss123!"       # change this!
EVENTHUB_NS="telecom-events-$(date +%s | tail -c 5)"
EVENTHUB_NAME="telecom-events"
KEYVAULT_NAME="telecom-kv-$(date +%s | tail -c 5)"
ADF_NAME="telecom-insights-adf"

echo "================================================"
echo " TelecomInsights — Azure Setup"
echo " Resource Group: $RESOURCE_GROUP"
echo " Location:       $LOCATION"
echo "================================================"

# ── Step 1: Resource Group ────────────────────────────────────
echo ""
echo "Step 1: Creating Resource Group..."
az group create \
    --name $RESOURCE_GROUP \
    --location $LOCATION
echo "✓ Resource Group created"

# ── Step 2: ADLS Gen2 ────────────────────────────────────────
echo ""
echo "Step 2: Creating ADLS Gen2 Storage Account..."
az storage account create \
    --name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard_LRS \
    --kind StorageV2 \
    --enable-hierarchical-namespace true \
    --min-tls-version TLS1_2 \
    --allow-blob-public-access false

echo "Creating Bronze/Silver/Gold containers..."
for CONTAINER in bronze silver gold; do
    az storage container create \
        --name $CONTAINER \
        --account-name $STORAGE_ACCOUNT \
        --auth-mode login
    echo "  ✓ Container: $CONTAINER"
done

echo "Creating folder structure in Bronze..."
# Create placeholder files to establish folder structure
for FOLDER in cdrs customers recharges events; do
    az storage blob upload \
        --account-name $STORAGE_ACCOUNT \
        --container-name bronze \
        --name "$FOLDER/.gitkeep" \
        --data "" \
        --auth-mode login 2>/dev/null || true
    echo "  ✓ Folder: bronze/$FOLDER/"
done
echo "✓ ADLS Gen2 created: $STORAGE_ACCOUNT"

# ── Step 3: Azure SQL Database ───────────────────────────────
echo ""
echo "Step 3: Creating Azure SQL Server + Database..."
az sql server create \
    --name $SQL_SERVER \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --admin-user $SQL_ADMIN \
    --admin-password $SQL_PASSWORD

# Allow Azure services to access SQL
az sql server firewall-rule create \
    --resource-group $RESOURCE_GROUP \
    --server $SQL_SERVER \
    --name AllowAzureServices \
    --start-ip-address 0.0.0.0 \
    --end-ip-address 0.0.0.0

# Allow your current IP (for loading customers from local machine)
MY_IP=$(curl -s https://api.ipify.org)
az sql server firewall-rule create \
    --resource-group $RESOURCE_GROUP \
    --server $SQL_SERVER \
    --name MyLocalIP \
    --start-ip-address $MY_IP \
    --end-ip-address $MY_IP

az sql db create \
    --resource-group $RESOURCE_GROUP \
    --server $SQL_SERVER \
    --name $SQL_DB \
    --edition Basic \
    --capacity 5
echo "✓ Azure SQL created: $SQL_SERVER/$SQL_DB"
echo "  Admin: $SQL_ADMIN / $SQL_PASSWORD"

# ── Step 4: Event Hub ─────────────────────────────────────────
echo ""
echo "Step 4: Creating Event Hub Namespace + Hub..."
az eventhubs namespace create \
    --name $EVENTHUB_NS \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku Standard \
    --enable-auto-inflate true \
    --maximum-throughput-units 10

az eventhubs eventhub create \
    --name $EVENTHUB_NAME \
    --namespace-name $EVENTHUB_NS \
    --resource-group $RESOURCE_GROUP \
    --partition-count 32 \
    --message-retention 7

# Create consumer groups
for CG in databricks-streaming power-bi-streaming; do
    az eventhubs eventhub consumer-group create \
        --resource-group $RESOURCE_GROUP \
        --namespace-name $EVENTHUB_NS \
        --eventhub-name $EVENTHUB_NAME \
        --name $CG
    echo "  ✓ Consumer group: $CG"
done

# Enable Capture to ADLS Bronze
az eventhubs eventhub update \
    --name $EVENTHUB_NAME \
    --namespace-name $EVENTHUB_NS \
    --resource-group $RESOURCE_GROUP \
    --enable-capture true \
    --capture-interval 300 \
    --capture-size-limit 314572800 \
    --destination-name EventHubArchive.AzureBlockBlob \
    --storage-account "/subscriptions/$(az account show --query id -o tsv)/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT" \
    --blob-container bronze \
    --archive-name-format "events/{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}"
echo "✓ Event Hub created: $EVENTHUB_NS/$EVENTHUB_NAME"
echo "  Partitions: 32 | Retention: 7 days | Capture: enabled → Bronze/events/"

# ── Step 5: Key Vault ─────────────────────────────────────────
echo ""
echo "Step 5: Creating Key Vault + Adding Secrets..."
az keyvault create \
    --name $KEYVAULT_NAME \
    --resource-group $RESOURCE_GROUP \
    --location $LOCATION \
    --sku standard

# Get storage account key
STORAGE_KEY=$(az storage account keys list \
    --account-name $STORAGE_ACCOUNT \
    --resource-group $RESOURCE_GROUP \
    --query '[0].value' -o tsv)

# Get Event Hub connection string
EH_CONN=$(az eventhubs namespace authorization-rule keys list \
    --resource-group $RESOURCE_GROUP \
    --namespace-name $EVENTHUB_NS \
    --name RootManageSharedAccessKey \
    --query primaryConnectionString -o tsv)

# Store secrets in Key Vault
az keyvault secret set --vault-name $KEYVAULT_NAME \
    --name "adls-storage-key"           --value "$STORAGE_KEY"
az keyvault secret set --vault-name $KEYVAULT_NAME \
    --name "sql-connection-string"      --value "Server=$SQL_SERVER.database.windows.net;Database=$SQL_DB;User Id=$SQL_ADMIN;Password=$SQL_PASSWORD;"
az keyvault secret set --vault-name $KEYVAULT_NAME \
    --name "eventhub-connection-string" --value "$EH_CONN"
az keyvault secret set --vault-name $KEYVAULT_NAME \
    --name "sql-password"               --value "$SQL_PASSWORD"
echo "✓ Key Vault created: $KEYVAULT_NAME"
echo "  Secrets stored: adls-storage-key, sql-connection-string, eventhub-connection-string"

# ── Step 6: Azure Data Factory ────────────────────────────────
echo ""
echo "Step 6: Creating Azure Data Factory..."
az datafactory create \
    --resource-group $RESOURCE_GROUP \
    --factory-name $ADF_NAME \
    --location $LOCATION
echo "✓ ADF created: $ADF_NAME"

# Grant ADF managed identity access to Key Vault
ADF_PRINCIPAL=$(az datafactory show \
    --resource-group $RESOURCE_GROUP \
    --factory-name $ADF_NAME \
    --query identity.principalId -o tsv)

az keyvault set-policy \
    --name $KEYVAULT_NAME \
    --object-id $ADF_PRINCIPAL \
    --secret-permissions get list

# Grant ADF managed identity access to ADLS
az role assignment create \
    --role "Storage Blob Data Contributor" \
    --assignee $ADF_PRINCIPAL \
    --scope "/subscriptions/$(az account show --query id -o tsv)/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"
echo "  ✓ ADF managed identity granted Key Vault + ADLS access"

# ── Step 7: Upload generated data ─────────────────────────────
echo ""
echo "Step 7: Uploading generated data to ADLS Bronze..."
if [ -d "data_generation/output" ]; then
    az storage blob upload-batch \
        --destination bronze \
        --source data_generation/output/ \
        --account-name $STORAGE_ACCOUNT \
        --auth-mode login \
        --pattern "**/*.csv"
    echo "✓ Data uploaded to ADLS Bronze"
else
    echo "⚠ data_generation/output/ not found"
    echo "  Run: cd data_generation && python run_all.py"
    echo "  Then re-run: az storage blob upload-batch ..."
fi

# ── Summary ───────────────────────────────────────────────────
echo ""
echo "================================================"
echo " SETUP COMPLETE — Save these values!"
echo "================================================"
echo ""
echo "Resource Group:   $RESOURCE_GROUP"
echo "Storage Account:  $STORAGE_ACCOUNT"
echo "SQL Server:       $SQL_SERVER.database.windows.net"
echo "SQL Database:     $SQL_DB"
echo "SQL Admin:        $SQL_ADMIN"
echo "SQL Password:     $SQL_PASSWORD"
echo "Event Hub NS:     $EVENTHUB_NS"
echo "Event Hub:        $EVENTHUB_NAME"
echo "Key Vault:        $KEYVAULT_NAME"
echo "ADF:              $ADF_NAME"
echo ""
echo "Next steps:"
echo "  1. Run SQL setup:  sql/01_create_tables.sql in Azure SQL Query Editor"
echo "  2. Load customers: python infrastructure/load_customers_to_sql.py"
echo "  3. Set up ADF pipelines: see adf_pipelines/pipeline_guide.md"
echo "================================================"
