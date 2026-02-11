# Databricks notebook source
# Load gold_dim_time from silver_time
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_dim_time AS
SELECT 
    sk_time_id AS sk_time_id,
    sk_time_id AS time_id,
    time_value,
    hour_id,
    hour_desc,
    minute_id,
    minute_desc,
    second_id,
    second_desc,
    market_hours_flag,
    office_hours_flag,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_time
WHERE batch_id = {batch_id}
""")
