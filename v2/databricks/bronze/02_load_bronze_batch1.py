# Databricks notebook source
# MAGIC %md
# MAGIC # Load Bronze Batch 1 Data
# MAGIC
# MAGIC This notebook loads raw files from Batch1/ directory into Bronze tables.

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
        # But if there's another error, we should know about it
        if "already exists" not in str(e).lower() and "table" not in str(e).lower():
            print(f"Warning: Error creating {table_name}: {e}")
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Reference Data (Batch 1 only)

# COMMAND ----------

# COMMAND ----------

# Set catalog and create/use schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

# Load Date.txt
spark.sql(f"""
CREATE OR REPLACE TABLE bronze_date AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Date.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/Date.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != ''
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load Time.txt
CREATE OR REPLACE TABLE bronze_time AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Time.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/Time.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load StatusType.txt
CREATE OR REPLACE TABLE bronze_status_type AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'StatusType.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/StatusType.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load TradeType.txt
CREATE OR REPLACE TABLE bronze_trade_type AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'TradeType.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/TradeType.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load Industry.txt
CREATE OR REPLACE TABLE bronze_industry AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Industry.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/Industry.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load TaxRate.txt
CREATE OR REPLACE TABLE bronze_tax_rate AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'TaxRate.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/TaxRate.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Brokerage Data (Batch 1: XML)

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load CustomerMgmt.xml (XML file - use spark-xml or native XML reader)
CREATE OR REPLACE TABLE bronze_customer_mgmt AS
SELECT 
    _c0 AS raw_xml,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'CustomerMgmt.xml' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/CustomerMgmt.xml', format => 'xml', rowTag => 'Customer');
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Market Data (Fixed-Width FINWIRE files - Batch 1 only)

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load FINWIRE files (multiple files: FINWIRE1967Q1.txt, FINWIRE1967Q2.txt, etc.)
CREATE OR REPLACE TABLE bronze_finwire AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    input_file_name() AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/FINWIRE*.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND length(value) >= 18;  -- Ensure minimum length for record type
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Transaction Data (Batch 1)

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load Trade.txt
CREATE OR REPLACE TABLE bronze_trade AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Trade.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/Trade.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load DailyMarket.txt
CREATE OR REPLACE TABLE bronze_daily_market AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'DailyMarket.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/DailyMarket.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load CashTransaction.txt
CREATE OR REPLACE TABLE bronze_cash_transaction AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'CashTransaction.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/CashTransaction.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load HoldingHistory.txt
CREATE OR REPLACE TABLE bronze_holding_history AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'HoldingHistory.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/HoldingHistory.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load WatchHistory.txt
CREATE OR REPLACE TABLE bronze_watch_history AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'WatchHistory.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/WatchHistory.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Other Sources (Batch 1)

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load HR.csv
CREATE OR REPLACE TABLE bronze_hr AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'HR.csv' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/HR.csv', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- Load Prospect.csv
CREATE OR REPLACE TABLE bronze_prospect AS
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Prospect.csv' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/Prospect.csv', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     'bronze_date' AS table_name,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM ${var.catalog}.${var.schema}.bronze_date
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_trade', COUNT(*) FROM ${var.catalog}.${var.schema}.bronze_trade WHERE _batch_id = ${var.batch_id}
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_daily_market', COUNT(*) FROM ${var.catalog}.${var.schema}.bronze_daily_market WHERE _batch_id = ${var.batch_id}
# MAGIC UNION ALL
# MAGIC SELECT 'bronze_finwire', COUNT(*) FROM ${var.catalog}.${var.schema}.bronze_finwire WHERE _batch_id = ${var.batch_id};
