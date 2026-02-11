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
dbutils.widgets.text("xml_format", "com.databricks.spark.xml", "XML Format (xml, com.databricks.spark.xml, or org.apache.spark.sql.execution.datasources.xml)")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = int(dbutils.widgets.get("batch_id"))
xml_format = dbutils.widgets.get("xml_format") or "com.databricks.spark.xml"

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

# Load CustomerMgmt.xml using PySpark with schema (referencing v1 implementation)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType, TimestampType
)

def get_customer_mgmt_schema():
    """CustomerMgmt XML schema definition (from v1 customer_mgmt_schema_definition.py)"""
    return StructType([
        StructField("Customer", StructType([
            StructField("Account", StructType([
                StructField("CA_B_ID", LongType(), True),
                StructField("CA_NAME", StringType(), True),
                StructField("_CA_ID", LongType(), True),
                StructField("_CA_TAX_ST", LongType(), True),
            ]), True),
            StructField("Address", StructType([
                StructField("C_ADLINE1", StringType(), True),
                StructField("C_ADLINE2", StringType(), True),
                StructField("C_CITY", StringType(), True),
                StructField("C_CTRY", StringType(), True),
                StructField("C_STATE_PROV", StringType(), True),
                StructField("C_ZIPCODE", StringType(), True),
            ]), True),
            StructField("ContactInfo", StructType([
                StructField("C_ALT_EMAIL", StringType(), True),
                StructField("C_PHONE_1", StructType([
                    StructField("C_AREA_CODE", LongType(), True),
                    StructField("C_CTRY_CODE", LongType(), True),
                    StructField("C_EXT", LongType(), True),
                    StructField("C_LOCAL", StringType(), True),
                ]), True),
                StructField("C_PHONE_2", StructType([
                    StructField("C_AREA_CODE", LongType(), True),
                    StructField("C_CTRY_CODE", LongType(), True),
                    StructField("C_EXT", LongType(), True),
                    StructField("C_LOCAL", StringType(), True),
                ]), True),
                StructField("C_PHONE_3", StructType([
                    StructField("C_AREA_CODE", LongType(), True),
                    StructField("C_CTRY_CODE", LongType(), True),
                    StructField("C_EXT", LongType(), True),
                    StructField("C_LOCAL", StringType(), True),
                ]), True),
                StructField("C_PRIM_EMAIL", StringType(), True),
            ]), True),
            StructField("Name", StructType([
                StructField("C_F_NAME", StringType(), True),
                StructField("C_L_NAME", StringType(), True),
                StructField("C_M_NAME", StringType(), True),
            ]), True),
            StructField("TaxInfo", StructType([
                StructField("C_LCL_TX_ID", StringType(), True),
                StructField("C_NAT_TX_ID", StringType(), True),
            ]), True),
            StructField("_C_DOB", DateType(), True),
            StructField("_C_GNDR", StringType(), True),
            StructField("_C_ID", LongType(), True),
            StructField("_C_TAX_ID", StringType(), True),
            StructField("_C_TIER", StringType(), True),
        ]), True),
        StructField("_ActionTS", TimestampType(), True),
        StructField("_ActionType", StringType(), True),
    ])

# Read XML file with schema (like v1) for validation, then read as text for raw storage
xml_path = f"{full_raw_data_path}/Batch1/CustomerMgmt.xml"
schema = get_customer_mgmt_schema()

# First, validate XML can be read with schema (like v1 does)
df_xml_validation = None
for row_tag, root_tag in [("TPCDI:Action", "TPCDI:Actions"), ("Action", None)]:
    try:
        reader = spark.read.format(xml_format)
        reader = reader.option("rowTag", row_tag)
        if root_tag:
            reader = reader.option("rootTag", root_tag)
        reader = reader.schema(schema)
        df_xml_validation = reader.load(xml_path)
        print(f"Successfully validated CustomerMgmt.xml with schema (rowTag={row_tag}, rootTag={root_tag})")
        break
    except Exception as e:
        print(f"Schema validation failed with rowTag={row_tag}, rootTag={root_tag}: {e}")
        # Try without schema for validation
        try:
            reader = spark.read.format(xml_format)
            reader = reader.option("rowTag", row_tag)
            if root_tag:
                reader = reader.option("rootTag", root_tag)
            df_xml_validation = reader.load(xml_path)
            print(f"Successfully validated CustomerMgmt.xml with inference (rowTag={row_tag})")
            break
        except Exception as e2:
            print(f"Validation failed: {e2}")
            continue

if df_xml_validation is None:
    raise RuntimeError(f"Failed to read/validate CustomerMgmt.xml from {xml_path}")

# For bronze layer, read XML file as text and extract individual action elements as raw XML strings
# Read the entire XML file as text (wholetext=True reads entire file as single row)
from pyspark.sql.functions import lit, current_timestamp, regexp_extract_all, explode, col, when

# Read XML file as text (whole file content)
df_xml_text = spark.read.option("wholetext", "true").text(xml_path)

# Extract individual action elements using regex
# Try namespaced tag first: <TPCDI:Action>...</TPCDI:Action>
# Note: regexp_extract_all with dotall flag (handles multi-line) - Spark regex supports (?s) flag
df_actions = df_xml_text.select(
    explode(
        regexp_extract_all(
            col("value"), 
            r"(?s)<TPCDI:Action[^>]*>.*?</TPCDI:Action>", 
            0
        )
    ).alias("raw_xml")
).filter(col("raw_xml").isNotNull() & (col("raw_xml") != ""))

# If no matches, try without namespace
action_count = df_actions.count()
if action_count == 0:
    df_actions = df_xml_text.select(
        explode(
            regexp_extract_all(
                col("value"), 
                r"(?s)<Action[^>]*>.*?</Action>", 
                0
            )
        ).alias("raw_xml")
    ).filter(col("raw_xml").isNotNull() & (col("raw_xml") != ""))

# Add metadata columns
df_bronze = df_actions.select(
    col("raw_xml"),
    lit(batch_id).alias("_batch_id"),
    current_timestamp().alias("_load_timestamp"),
    lit("CustomerMgmt.xml").alias("_source_file")
)

# Write to bronze table
df_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.bronze_customer_mgmt")
print(f"Successfully loaded {df_bronze.count()} rows into bronze_customer_mgmt")

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
