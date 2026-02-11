# Databricks notebook source
# Transform bronze_time -> silver_time (pipe-delimited, time_value as STRING)
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_time AS
SELECT 
    CAST(split(raw_line, '\\\\|')[0] AS INT) AS sk_time_id,
    split(raw_line, '\\\\|')[1] AS time_value,
    CAST(split(raw_line, '\\\\|')[2] AS INT) AS hour_id,
    split(raw_line, '\\\\|')[3] AS hour_desc,
    CAST(split(raw_line, '\\\\|')[4] AS INT) AS minute_id,
    split(raw_line, '\\\\|')[5] AS minute_desc,
    CAST(split(raw_line, '\\\\|')[6] AS INT) AS second_id,
    split(raw_line, '\\\\|')[7] AS second_desc,
    CAST(split(raw_line, '\\\\|')[8] AS BOOLEAN) AS market_hours_flag,
    CAST(split(raw_line, '\\\\|')[9] AS BOOLEAN) AS office_hours_flag,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_time
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
""")
