# Databricks notebook source
# Transform bronze_trade -> silver_trades (pipe-delimited, 16 cols historical)
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

sql = f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_trades AS
SELECT 
    CAST(split(raw_line, '\\\\|')[0] AS BIGINT) AS trade_id,
    CAST(split(raw_line, '\\\\|')[1] AS TIMESTAMP) AS trade_dts,
    split(raw_line, '\\\\|')[2] AS status_id,
    split(raw_line, '\\\\|')[3] AS trade_type_id,
    CAST(split(raw_line, '\\\\|')[4] AS BOOLEAN) AS is_cash,
    split(raw_line, '\\\\|')[5] AS symbol,
    CAST(split(raw_line, '\\\\|')[6] AS INT) AS quantity,
    CAST(split(raw_line, '\\\\|')[7] AS DOUBLE) AS bid_price,
    CAST(split(raw_line, '\\\\|')[8] AS BIGINT) AS account_id,
    split(raw_line, '\\\\|')[9] AS exec_name,
    CAST(split(raw_line, '\\\\|')[10] AS DOUBLE) AS trade_price,
    CAST(split(raw_line, '\\\\|')[11] AS DOUBLE) AS charge,
    CAST(split(raw_line, '\\\\|')[12] AS DOUBLE) AS commission,
    CAST(split(raw_line, '\\\\|')[13] AS DOUBLE) AS tax,
    TRUE AS is_current,
    CAST(split(raw_line, '\\\\|')[1] AS TIMESTAMP) AS effective_date,
    NULL AS end_date,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM {catalog}.{schema_name}.bronze_trade
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\\\|')) = 16
"""
spark.sql(sql)
