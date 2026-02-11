# Databricks notebook source
# Load gold_dim_trade_type from silver_trade_type (widgets set by orchestrator)
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
