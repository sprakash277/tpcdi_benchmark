# Databricks notebook source
# MAGIC %md
# MAGIC # Load Bronze Incremental Data
# MAGIC
# MAGIC Loads incremental data into Bronze tables

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw Data Path")
dbutils.widgets.text("sf", "10", "Scale Factor")
dbutils.widgets.text("batch_id", "1", "Batch ID")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = int(dbutils.widgets.get("batch_id"))

# Construct full path with sf appended
full_raw_data_path = f"{raw_data_path}/sf={sf}"

# Set SQL variables
spark.sql(f"SET var.catalog = '{catalog}'")
spark.sql(f"SET var.schema = '{schema_name}'")
spark.sql(f"SET var.raw_data_path = '{full_raw_data_path}'")
spark.sql(f"SET var.batch_id = {batch_id}")
spark.sql(f"SET var.sf = {sf}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Tables
# MAGIC
# MAGIC Create all bronze tables before loading data.

# COMMAND ----------

# Get the current notebook path to determine the base path for table creation notebooks
import os
current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
base_path = os.path.dirname(current_notebook_path)
tables_path = f"{base_path}/tables"

# List of all bronze tables to create (in order)
bronze_tables = [
    "bronze_date",
    "bronze_time", 
    "bronze_status_type",
    "bronze_trade_type",
    "bronze_industry",
    "bronze_tax_rate",
    "bronze_hr",
    "bronze_customer_mgmt",
    "bronze_customer",
    "bronze_account",
    "bronze_trade",
    "bronze_daily_market",
    "bronze_prospect",
    "bronze_cash_transaction",
    "bronze_holding_history",
    "bronze_watch_history",
    "bronze_finwire"
]

# Create all bronze tables
for table_name in bronze_tables:
    create_notebook = f"{tables_path}/create_{table_name}"
    print(f"Creating table: {table_name} via {create_notebook}")
    try:
        dbutils.notebook.run(create_notebook, timeout_seconds=300, arguments={
            "catalog": catalog,
            "schema_name": schema_name
        })
    except Exception as e:
        # If table already exists, that's okay (CREATE TABLE IF NOT EXISTS handles this)
        if "already exists" not in str(e).lower() and "table" not in str(e).lower():
            print(f"Warning: Error creating {table_name}: {e}")
            raise

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- TPC-DI v2: Bronze Layer - Incremental Load (Batch 2+)
# MAGIC -- ============================================================================
# MAGIC -- Loads raw files from Batch{N}/ directory into Bronze tables
# MAGIC -- Uses APPEND mode to add incremental data
# MAGIC -- ============================================================================
# MAGIC -- Set variables (adjust paths and batch_id as needed)
# MAGIC -- SET var.raw_data_path = '/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10';
# MAGIC -- SET var.batch_id = 2;  -- Change for Batch 3, 4, etc.
# MAGIC -- ============================================================================
# MAGIC -- Brokerage Data (Batch 2+: Pipe-delimited)
# MAGIC -- ============================================================================
# COMMAND ----------

# Set catalog and create/use schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load Customer.txt (incremental)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_customer (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     ${var.batch_id} AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'Customer.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/Customer.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load Account.txt (incremental)
# MAGIC INSERT INTO bronze_account (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     ${var.batch_id} AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'Account.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/Account.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Transaction Data (Batch 2+: All batches)
# MAGIC -- ============================================================================
# MAGIC -- Load Trade.txt (incremental)
# MAGIC INSERT INTO bronze_trade (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     ${var.batch_id} AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'Trade.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/Trade.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load DailyMarket.txt (incremental)
# MAGIC INSERT INTO bronze_daily_market (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     ${var.batch_id} AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'DailyMarket.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/DailyMarket.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load CashTransaction.txt (incremental)
# MAGIC INSERT INTO bronze_cash_transaction (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     ${var.batch_id} AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'CashTransaction.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/CashTransaction.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load HoldingHistory.txt (incremental)
# MAGIC INSERT INTO bronze_holding_history (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     ${var.batch_id} AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'HoldingHistory.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/HoldingHistory.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load WatchHistory.txt (incremental)
# MAGIC INSERT INTO bronze_watch_history (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     ${var.batch_id} AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'WatchHistory.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/WatchHistory.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Other Sources (Batch 2+: Prospect only)
# MAGIC -- ============================================================================
# MAGIC -- Load Prospect.csv (incremental)
# MAGIC INSERT INTO bronze_prospect (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     ${var.batch_id} AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'Prospect.csv' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/Prospect.csv', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Load completed' AS status;