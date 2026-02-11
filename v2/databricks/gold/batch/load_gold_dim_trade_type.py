# Databricks notebook source
# Load gold_dim_trade_type from silver_trade_type
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_dim_trade_type AS
SELECT 
    tt_id AS sk_trade_type_id,
    tt_id AS trade_type_id,
    tt_id AS trade_type_code,
    tt_name AS trade_type_name,
    tt_is_sell AS is_sell,
    tt_is_mrkt AS is_market,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_trade_type
WHERE batch_id = {batch_id}
""")
