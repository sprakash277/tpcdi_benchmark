# Databricks notebook source
# Transform bronze_finwire (SEC) -> silver_securities (fixed-width)
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_securities AS
SELECT 
    TRIM(substring(raw_line, 19, 15)) AS symbol,
    TRIM(substring(raw_line, 34, 6)) AS issue_type,
    TRIM(substring(raw_line, 40, 4)) AS status,
    TRIM(substring(raw_line, 44, 70)) AS name,
    TRIM(substring(raw_line, 114, 6)) AS ex_id,
    CAST(TRIM(substring(raw_line, 120, 13)) AS BIGINT) AS sh_out,
    try_to_date(substring(raw_line, 133, 8), 'yyyyMMdd') AS first_trade_date,
    TRIM(substring(raw_line, 141, 8)) AS first_trade_exchg,
    CAST(TRIM(substring(raw_line, 149, 12)) AS DOUBLE) AS dividend,
    TRIM(substring(raw_line, 161, 60)) AS co_name_or_cik,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_finwire
WHERE _batch_id = {batch_id}
  AND substring(raw_line, 16, 3) = 'SEC'
  AND length(raw_line) >= 220
""")
