# Databricks notebook source
# Load bronze_watch_history from Batch1/WatchHistory.txt
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw Data Path")
dbutils.widgets.text("sf", "10", "Scale Factor")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = int(dbutils.widgets.get("batch_id"))
full_raw_data_path = f"{raw_data_path}/sf={sf}"

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.bronze_watch_history AS
SELECT 
    value AS raw_line,
    {batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'WatchHistory.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch1/WatchHistory.txt', format => 'text', lineSep => '\\n')
WHERE value IS NOT NULL AND value != ''
""")
