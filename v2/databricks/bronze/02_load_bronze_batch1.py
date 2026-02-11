# Databricks notebook source
# MAGIC %md
# MAGIC # Load Bronze Batch 1 Data
# MAGIC
# MAGIC This notebook loads raw files from Batch1/ directory into Bronze tables.

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("raw_data_path", "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10", "Raw Data Path")
dbutils.widgets.text("batch_id", "1", "Batch ID")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
batch_id = int(dbutils.widgets.get("batch_id"))

# Set SQL variables
spark.sql(f"SET var.catalog = '{catalog}'")
spark.sql(f"SET var.schema = '{schema_name}'")
spark.sql(f"SET var.raw_data_path = '{raw_data_path}'")
spark.sql(f"SET var.batch_id = {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Reference Data (Batch 1 only)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load Date.txt
# MAGIC USE CATALOG ${var.catalog};
# MAGIC USE SCHEMA ${var.schema};
# MAGIC
# MAGIC INSERT INTO bronze_date (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'Date.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/Date.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load Time.txt
# MAGIC INSERT INTO bronze_time (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'Time.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/Time.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load StatusType.txt
# MAGIC INSERT INTO bronze_status_type (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'StatusType.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/StatusType.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load TradeType.txt
# MAGIC INSERT INTO bronze_trade_type (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'TradeType.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/TradeType.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load Industry.txt
# MAGIC INSERT INTO bronze_industry (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'Industry.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/Industry.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load TaxRate.txt
# MAGIC INSERT INTO bronze_tax_rate (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'TaxRate.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/TaxRate.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Brokerage Data (Batch 1: XML)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load CustomerMgmt.xml (XML file - use spark-xml or native XML reader)
# MAGIC INSERT INTO bronze_customer_mgmt (raw_xml, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     _c0 AS raw_xml,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'CustomerMgmt.xml' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/CustomerMgmt.xml', format => 'xml', rowTag => 'Customer');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Market Data (Fixed-Width FINWIRE files - Batch 1 only)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load FINWIRE files (multiple files: FINWIRE1967Q1.txt, FINWIRE1967Q2.txt, etc.)
# MAGIC INSERT INTO bronze_finwire (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     input_file_name() AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/FINWIRE*.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND length(value) >= 18;  -- Ensure minimum length for record type

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Transaction Data (Batch 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load Trade.txt
# MAGIC INSERT INTO bronze_trade (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'Trade.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/Trade.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load DailyMarket.txt
# MAGIC INSERT INTO bronze_daily_market (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'DailyMarket.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/DailyMarket.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load CashTransaction.txt
# MAGIC INSERT INTO bronze_cash_transaction (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'CashTransaction.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/CashTransaction.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load HoldingHistory.txt
# MAGIC INSERT INTO bronze_holding_history (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'HoldingHistory.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/HoldingHistory.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load WatchHistory.txt
# MAGIC INSERT INTO bronze_watch_history (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'WatchHistory.txt' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/WatchHistory.txt', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Other Sources (Batch 1)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load HR.csv
# MAGIC INSERT INTO bronze_hr (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'HR.csv' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/HR.csv', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Load Prospect.csv
# MAGIC INSERT INTO bronze_prospect (raw_line, _batch_id, _load_timestamp, _source_file)
# MAGIC SELECT 
# MAGIC     value AS raw_line,
# MAGIC     1 AS _batch_id,
# MAGIC     current_timestamp() AS _load_timestamp,
# MAGIC     'Prospect.csv' AS _source_file
# MAGIC FROM read_files('${var.raw_data_path}/Batch1/Prospect.csv', format => 'text', lineSep => '\n')
# MAGIC WHERE value IS NOT NULL AND value != '';

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
