# Databricks notebook source
# Transform bronze_daily_market -> silver_daily_market (6 cols historical)
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

sql = f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_daily_market AS
SELECT 
    CONCAT(CAST(split(raw_line, '\\\\|')[0] AS DATE), '|', split(raw_line, '\\\\|')[1]) AS dm_key,
    CAST(split(raw_line, '\\\\|')[0] AS DATE) AS dm_date,
    split(raw_line, '\\\\|')[1] AS dm_s_symb,
    CAST(split(raw_line, '\\\\|')[2] AS DOUBLE) AS dm_close,
    CAST(split(raw_line, '\\\\|')[3] AS DOUBLE) AS dm_high,
    CAST(split(raw_line, '\\\\|')[4] AS DOUBLE) AS dm_low,
    CAST(split(raw_line, '\\\\|')[5] AS BIGINT) AS dm_vol,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_daily_market
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\\\|')) = 6
"""
spark.sql(sql)
