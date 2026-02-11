# Databricks notebook source
# Transform bronze_trade_type -> silver_trade_type
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_trade_type AS
SELECT 
    split(raw_line, '\\\\|')[0] AS tt_id,
    split(raw_line, '\\\\|')[1] AS tt_name,
    CAST(split(raw_line, '\\\\|')[2] AS BOOLEAN) AS tt_is_sell,
    CAST(split(raw_line, '\\\\|')[3] AS BOOLEAN) AS tt_is_mrkt,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_trade_type
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
""")
