# How to Run TPC-DI v2 SQL Implementation

This guide provides step-by-step instructions to execute the v2 SQL-only implementation.

## Prerequisites

### Databricks
- Databricks workspace with Unity Catalog enabled
- Access to TPC-DI data files (in DBFS/Volumes or external storage)
- SQL Warehouse or Cluster with Delta Lake support

### Dataproc
- Google Cloud Project with Dataproc cluster
- TPC-DI data files uploaded to GCS bucket
- Delta Lake JARs configured on cluster
- spark-xml JAR for XML parsing: `com.databricks:spark-xml_2.12:0.18.0`

---

## Option 1: Databricks Execution

### Step 1: Set Up Environment

1. **Create Catalog and Schemas** (if not exists):
```sql
CREATE CATALOG IF NOT EXISTS tpcdi_catalog;
USE CATALOG tpcdi_catalog;

CREATE SCHEMA IF NOT EXISTS bronze_schema;
CREATE SCHEMA IF NOT EXISTS silver_schema;
CREATE SCHEMA IF NOT EXISTS gold_schema;
```

2. **Set Variables**:
```sql
SET var.raw_data_path = '/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10';
SET var.batch_id = 1;
```

**Note**: Adjust `var.raw_data_path` to match your data location:
- Unity Catalog Volume: `/Volumes/catalog/schema/volume_name/path`
- DBFS: `/dbfs/mnt/path/to/data`
- External: `s3://bucket/path` or `abfss://container@account.dfs.core.windows.net/path`

### Step 2: Execute Batch 1 (Historical Load)

#### 2.1 Bronze Layer
```sql
-- Switch to bronze schema
USE CATALOG tpcdi_catalog;
USE SCHEMA bronze_schema;

-- Load Batch 1 data (tables created in load notebook or via tables/create_*.py)
-- Copy and execute: v2/databricks/bronze/02_load_bronze_batch1.sql (or .py notebook)
```

#### 2.2 Silver Layer
```sql
-- Switch to silver schema
USE CATALOG tpcdi_catalog;
USE SCHEMA silver_schema;

-- Transform Bronze → Silver (Batch 1)
-- Copy and execute: v2/databricks/silver/02_transform_silver_batch1.sql (or .py notebook)
```

#### 2.3 Gold Layer
```sql
-- Switch to gold schema
USE CATALOG tpcdi_catalog;
USE SCHEMA gold_schema;

-- Load Silver → Gold (Batch 1)
-- Copy and execute: v2/databricks/gold/02_load_gold_batch1.sql (or .py notebook)
```

### Step 3: Execute Incremental Loads (Batch 2+)

For each incremental batch (2, 3, etc.):

```sql
-- Set batch ID
SET var.batch_id = 2;  -- Change for Batch 3, 4, etc.

-- Bronze Layer
USE CATALOG tpcdi_catalog;
USE SCHEMA bronze_schema;
-- Execute: v2/databricks/bronze/03_load_bronze_incremental.sql

-- Silver Layer
USE SCHEMA silver_schema;
-- Execute: v2/databricks/silver/03_transform_silver_incremental.sql

-- Gold Layer
USE SCHEMA gold_schema;
-- Execute: v2/databricks/gold/03_load_gold_incremental.sql
```

### Execution Methods in Databricks

#### Method A: SQL Editor (Recommended)
1. Open Databricks SQL Editor
2. Copy SQL from each file
3. Paste and execute
4. Variables persist within the same session

#### Method B: Notebook
1. Create a new SQL notebook
2. Copy SQL files into cells
3. Run cells sequentially
4. Variables persist across cells

#### Method C: Workflow/Job
1. Create a Databricks Workflow
2. Add SQL tasks for each file
3. Set variables in task parameters
4. Schedule or trigger manually

---

## Option 2: Dataproc Execution

### Step 1: Set Up Environment

1. **Create GCS Bucket** (if not exists):
```bash
gsutil mb -p YOUR_PROJECT_ID gs://YOUR_BUCKET
```

2. **Upload TPC-DI Data**:
```bash
gsutil -m cp -r /path/to/tpcdi/data/* gs://YOUR_BUCKET/tpcdi/sf=10/
```

3. **Create Databases**:
```sql
CREATE DATABASE IF NOT EXISTS tpcdi_bronze;
CREATE DATABASE IF NOT EXISTS tpcdi_silver;
CREATE DATABASE IF NOT EXISTS tpcdi_gold;
```

4. **Set Variables**:
```sql
SET var.raw_data_path = 'gs://YOUR_BUCKET/tpcdi/sf=10';
SET var.batch_id = 1;
```

**Important**: Update `YOUR_BUCKET` in all SQL files before execution!

### Step 2: Update GCS Paths in SQL Files

Before running, replace `YOUR_BUCKET` in all Dataproc SQL files:

```bash
# Example: Update bronze table locations
sed -i 's/YOUR_BUCKET/your-actual-bucket-name/g' v2/dataproc/bronze/01_create_bronze_tables.sql
sed -i 's/YOUR_BUCKET/your-actual-bucket-name/g' v2/dataproc/silver/01_create_silver_tables.sql
sed -i 's/YOUR_BUCKET/your-actual-bucket-name/g' v2/dataproc/gold/01_create_gold_tables.sql
```

### Step 3: Execute Batch 1 (Historical Load)

#### 3.1 Bronze Layer
```sql
USE DATABASE tpcdi_bronze;

-- Create Bronze tables
-- Execute: v2/dataproc/bronze/01_create_bronze_tables.sql

-- Load Batch 1 data
-- Execute: v2/dataproc/bronze/02_load_bronze_batch1.sql
```

#### 3.2 Silver Layer
```sql
USE DATABASE tpcdi_silver;

-- Create Silver tables
-- Execute: v2/dataproc/silver/01_create_silver_tables.sql

-- Transform Bronze → Silver (Batch 1)
-- Execute: v2/dataproc/silver/02_transform_silver_batch1.sql
```

#### 3.3 Gold Layer
```sql
USE DATABASE tpcdi_gold;

-- Create Gold tables
-- Execute: v2/dataproc/gold/01_create_gold_tables.sql

-- Load Silver → Gold (Batch 1)
-- Execute: v2/dataproc/gold/02_load_gold_batch1.sql
```

### Step 4: Execute Incremental Loads (Batch 2+)

```sql
SET var.batch_id = 2;

-- Bronze Layer
USE DATABASE tpcdi_bronze;
-- Execute: v2/dataproc/bronze/03_load_bronze_incremental.sql

-- Silver Layer
USE DATABASE tpcdi_silver;
-- Execute: v2/dataproc/silver/03_transform_silver_incremental.sql

-- Gold Layer
USE DATABASE tpcdi_gold;
-- Execute: v2/dataproc/gold/03_load_gold_incremental.sql
```

### Execution Methods in Dataproc

#### Method A: Spark SQL Shell
```bash
# SSH into Dataproc cluster
gcloud compute ssh CLUSTER_NAME --zone=ZONE

# Start Spark SQL shell
spark-sql \
  --jars gs://spark-lib/delta/delta-core_2.12-2.4.0.jar,gs://spark-lib/spark-xml_2.12-0.18.0.jar \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

# Then execute SQL files
```

#### Method B: Dataproc Jobs API
```bash
# Submit SQL job
gcloud dataproc jobs submit spark-sql \
  --cluster=CLUSTER_NAME \
  --region=REGION \
  --file=gs://YOUR_BUCKET/sql/v2/dataproc/bronze/01_create_bronze_tables.sql \
  --jars=gs://spark-lib/delta/delta-core_2.12-2.4.0.jar,gs://spark-lib/spark-xml_2.12-0.18.0.jar \
  --properties=spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

#### Method C: Jupyter Notebook on Dataproc
1. Connect to Dataproc Jupyter interface
2. Create SQL notebook
3. Copy and execute SQL files
4. Variables can be set in separate cells

---

## Quick Start Scripts

### Databricks Quick Start (Batch 1)

Create a notebook with this structure:

```python
# Cell 1: Setup
%sql
CREATE CATALOG IF NOT EXISTS tpcdi_catalog;
USE CATALOG tpcdi_catalog;
CREATE SCHEMA IF NOT EXISTS bronze_schema;
CREATE SCHEMA IF NOT EXISTS silver_schema;
CREATE SCHEMA IF NOT EXISTS gold_schema;

SET var.raw_data_path = '/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10';
SET var.batch_id = 1;
```

```sql
-- Cell 2: Bronze - Create Tables
-- Paste: v2/databricks/bronze/01_create_bronze_tables.sql
```

```sql
-- Cell 3: Bronze - Load Batch 1
-- Paste: v2/databricks/bronze/02_load_bronze_batch1.sql (or .py notebook)
```

```sql
-- Cell 4: Silver - Transform Batch 1
USE SCHEMA silver_schema;
-- Paste: v2/databricks/silver/02_transform_silver_batch1.sql (or .py notebook)
```

```sql
-- Cell 5: Gold - Load Batch 1
USE SCHEMA gold_schema;
-- Paste: v2/databricks/gold/02_load_gold_batch1.sql (or .py notebook)
```

### Dataproc Quick Start Script

Create `run_v2_batch1.sh`:

```bash
#!/bin/bash
BUCKET="your-bucket-name"
CLUSTER="your-cluster-name"
REGION="your-region"

# Update bucket name in SQL files
find v2/dataproc -name "*.sql" -exec sed -i "s/YOUR_BUCKET/$BUCKET/g" {} \;

# Submit jobs sequentially
gcloud dataproc jobs submit spark-sql \
  --cluster=$CLUSTER --region=$REGION \
  --file=v2/dataproc/bronze/01_create_bronze_tables.sql

gcloud dataproc jobs submit spark-sql \
  --cluster=$CLUSTER --region=$REGION \
  --file=v2/dataproc/bronze/02_load_bronze_batch1.sql

# ... continue for all files
```

---

## Verification

After execution, verify data loaded correctly:

```sql
-- Check Bronze row counts
SELECT COUNT(*) FROM bronze_customer_mgmt WHERE _batch_id = 1;
SELECT COUNT(*) FROM bronze_trade WHERE _batch_id = 1;

-- Check Silver row counts
SELECT COUNT(*) FROM silver_customers WHERE batch_id = 1;
SELECT COUNT(*) FROM silver_trades WHERE batch_id = 1;

-- Check Gold row counts
SELECT COUNT(*) FROM gold_dim_customer;
SELECT COUNT(*) FROM gold_fact_trade;

-- Check for data quality issues
SELECT * FROM gold_dim_messages ORDER BY message_timestamp DESC LIMIT 100;
```

---

## Troubleshooting

### Common Issues

1. **Variable not found**: Ensure `SET var.xxx` is executed before using variables
2. **Table not found**: Check schema/catalog context with `USE CATALOG/SCHEMA`
3. **File not found**: Verify `var.raw_data_path` points to correct location
4. **XML parsing error** (Dataproc): Ensure spark-xml JAR is included
5. **Delta Lake errors**: Verify Delta Lake JARs are configured correctly

### Debugging Tips

```sql
-- Check current context
SELECT current_catalog(), current_database();

-- List tables
SHOW TABLES;

-- Check table schema
DESCRIBE TABLE bronze_customer_mgmt;

-- Check variable value
SET var.raw_data_path;
```

---

## Next Steps

1. **Monitor Performance**: Check query execution times
2. **Optimize Tables**: Consider partitioning/Z-ordering for large tables
3. **Set Up Monitoring**: Create alerts on `gold_dim_messages` table
4. **Schedule Incremental Loads**: Automate Batch 2+ execution
