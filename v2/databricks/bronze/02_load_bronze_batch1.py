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

# Load CustomerMgmt.xml using same logic as v1 (no UDTF): spark-xml with schema/format fallbacks, write parsed struct to bronze
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType, TimestampType
)
from pyspark.sql.functions import lit, current_timestamp

def get_customer_mgmt_schema():
    """CustomerMgmt XML schema (from v1 customer_mgmt_schema_definition.py)."""
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

xml_path = f"{full_raw_data_path}/Batch1/CustomerMgmt.xml"
schema = get_customer_mgmt_schema()
fmt = (xml_format or "xml").strip() or "xml"
df = None
success = False

for row_tag, root_tag in [("TPCDI:Action", "TPCDI:Actions"), ("Action", None)]:
    if success:
        break
    try:
        reader = spark.read.format(fmt).option("rowTag", row_tag)
        if root_tag:
            reader = reader.option("rootTag", root_tag)
        if schema is not None:
            reader = reader.schema(schema)
        df = reader.load(xml_path)
        if df.count() > 0:
            print(f"Successfully read CustomerMgmt.xml with rowTag={row_tag}, format={fmt}")
            success = True
            break
        df = None
    except Exception as e:
        err_msg = str(e)
        if fmt == "com.databricks.spark.xml" and (
            "ServiceConfigurationError" in err_msg or "Unable to get public no-arg constructor" in err_msg
        ):
            print(f"Format com.databricks.spark.xml failed; falling back to 'xml'")
            fmt = "xml"
            try:
                reader = spark.read.format(fmt).option("rowTag", row_tag)
                if root_tag:
                    reader = reader.option("rootTag", root_tag)
                if schema is not None:
                    reader = reader.schema(schema)
                df = reader.load(xml_path)
                if df.count() > 0:
                    print(f"Successfully read CustomerMgmt.xml with rowTag={row_tag}, format={fmt}")
                    success = True
                    break
            except Exception as e2:
                print(f"Fallback format 'xml' also failed: {e2}")
            if success:
                break
            df = None
            continue
        if schema is not None:
            print(f"Read with schema failed, will infer: {e}")
            schema = None
            continue
        print(f"Failed to read XML with rowTag={row_tag}: {e}")
        df = None

if not success or df is None:
    raise RuntimeError(
        f"Could not read CustomerMgmt.xml from {xml_path}. "
        "Ensure spark-xml is available (e.g. com.databricks:spark-xml_2.12:0.15.0)."
    )

# Add metadata columns (same as v1 _write_bronze_table)
df_bronze = df.withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit("CustomerMgmt.xml"))

# Drop table if exists before writing
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema_name}.bronze_customer_mgmt")

# Write parsed DataFrame to bronze (same as v1: store nested struct, not raw XML)
df_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.bronze_customer_mgmt")
print(f"Successfully loaded {df_bronze.count()} rows into bronze_customer_mgmt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Market Data (Fixed-Width FINWIRE files - Batch 1 only)

# COMMAND ----------

# COMMAND ----------

# Load FINWIRE files - same as v1: single path with FINWIRE* glob, text format, rename value -> raw_line, add metadata
from pyspark.sql.functions import lit, current_timestamp, col, length

file_pattern = f"{full_raw_data_path}/Batch1/FINWIRE*"
df_finwire = None
try:
    df_finwire = spark.read.format("text").load(file_pattern)
except Exception as e:
    if "Path does not exist" in str(e) or "Cannot find" in str(e) or "42K03" in str(e):
        # Fallback for GCS etc. where glob in path is not resolved: list files then load
        batch1_path = f"{full_raw_data_path}/Batch1"
        batch1_files = dbutils.fs.ls(batch1_path)
        finwire_files = [f.path for f in batch1_files if "FINWIRE" in f.name.upper() and (f.name.lower().endswith(".txt") or "." not in f.name)]
        if not finwire_files:
            raise FileNotFoundError(f"No FINWIRE files found under {batch1_path}. Error: {e}") from e
        df_finwire = spark.read.format("text").load(finwire_files)
    else:
        raise

df_finwire_bronze = df_finwire.withColumnRenamed("value", "raw_line") \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit("FINWIRE*")) \
    .filter(col("raw_line").isNotNull()).filter(col("raw_line") != "").filter(length(col("raw_line")) >= 18)

df_finwire_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.bronze_finwire")
print(f"Successfully loaded {df_finwire_bronze.count()} rows into bronze_finwire")

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

# Build full table names in Python to avoid SQL variable substitution adding quotes (parse error)
spark.sql(f"""
SELECT 
    'bronze_date' AS table_name,
    COUNT(*) AS row_count
FROM {catalog}.{schema_name}.bronze_date
WHERE _batch_id = {batch_id}
UNION ALL
SELECT 'bronze_trade', COUNT(*) FROM {catalog}.{schema_name}.bronze_trade WHERE _batch_id = {batch_id}
UNION ALL
SELECT 'bronze_daily_market', COUNT(*) FROM {catalog}.{schema_name}.bronze_daily_market WHERE _batch_id = {batch_id}
UNION ALL
SELECT 'bronze_finwire', COUNT(*) FROM {catalog}.{schema_name}.bronze_finwire WHERE _batch_id = {batch_id}
""").show()
