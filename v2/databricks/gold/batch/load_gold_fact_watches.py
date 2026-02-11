# Databricks notebook source
# Load gold_fact_watches from silver_watch_history
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_fact_watches AS
SELECT 
    dc.sk_customer_id,
    ds.sk_security_id,
    swh.w_c_id AS customer_id,
    swh.w_s_symb AS symbol,
    swh.w_dts AS watch_date,
    swh.w_action AS watch_action,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_watch_history swh
INNER JOIN {catalog}.{schema_name}.gold_dim_customer dc ON swh.w_c_id = dc.customer_id
INNER JOIN {catalog}.{schema_name}.gold_dim_security ds ON swh.w_s_symb = ds.symbol
WHERE swh.batch_id = {batch_id}
  AND swh.is_current = true
""")
