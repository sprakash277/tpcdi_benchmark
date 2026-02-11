# Databricks notebook source
# Transform bronze_finwire (FIN) -> silver_financials (fixed-width)
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_financials AS
SELECT 
    TRIM(substring(raw_line, 187, 60)) AS co_name_or_cik,
    CAST(TRIM(substring(raw_line, 19, 4)) AS INT) AS year,
    CAST(TRIM(substring(raw_line, 23, 1)) AS INT) AS quarter,
    try_to_date(substring(raw_line, 24, 8), 'yyyyMMdd') AS qtr_start_date,
    try_to_date(substring(raw_line, 32, 8), 'yyyyMMdd') AS posting_date,
    CAST(TRIM(substring(raw_line, 40, 17)) AS DOUBLE) AS revenue,
    CAST(TRIM(substring(raw_line, 57, 17)) AS DOUBLE) AS earnings,
    CAST(TRIM(substring(raw_line, 74, 12)) AS DOUBLE) AS eps,
    CAST(TRIM(substring(raw_line, 86, 12)) AS DOUBLE) AS diluted_eps,
    CAST(TRIM(substring(raw_line, 98, 12)) AS DOUBLE) AS margin,
    CAST(TRIM(substring(raw_line, 110, 17)) AS DOUBLE) AS inventory,
    CAST(TRIM(substring(raw_line, 127, 17)) AS DOUBLE) AS assets,
    CAST(TRIM(substring(raw_line, 144, 17)) AS DOUBLE) AS liabilities,
    CAST(TRIM(substring(raw_line, 161, 13)) AS BIGINT) AS sh_out,
    CAST(TRIM(substring(raw_line, 174, 13)) AS BIGINT) AS diluted_sh_out,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_finwire
WHERE _batch_id = {batch_id}
  AND substring(raw_line, 16, 3) = 'FIN'
  AND length(raw_line) >= 246
""")
