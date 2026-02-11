# Databricks notebook source
# Transform bronze_holding_history -> silver_holding_history (4 cols historical)
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

sql = f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_holding_history AS
SELECT 
    CAST(split(raw_line, '\\\\|')[0] AS BIGINT) AS hh_h_t_id,
    CAST(split(raw_line, '\\\\|')[1] AS BIGINT) AS hh_t_id,
    CAST(split(raw_line, '\\\\|')[2] AS INT) AS hh_before_qty,
    CAST(split(raw_line, '\\\\|')[3] AS INT) AS hh_after_qty,
    TRUE AS is_current,
    current_timestamp() AS effective_date,
    NULL AS end_date,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM {catalog}.{schema_name}.bronze_holding_history
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\\\|')) = 4
"""
spark.sql(sql)
