# Databricks notebook source
# Load bronze_holding_history from Batch1/HoldingHistory.txt (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = int(dbutils.widgets.get("batch_id"))
full_raw_data_path = f"{raw_data_path}/sf={sf}"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.bronze_holding_history AS
SELECT 
    value AS raw_line,
    {batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'HoldingHistory.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/HoldingHistory.txt', format => 'text', lineSep => '\\n')
WHERE value IS NOT NULL AND value != ''
""")
