# Databricks notebook source
# Transform bronze_finwire (CMP) -> silver_companies (fixed-width)
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_companies AS
SELECT 
    monotonically_increasing_id() AS sk_company_id,
    TRIM(substring(raw_line, 79, 10)) AS company_id,
    TRIM(substring(raw_line, 19, 60)) AS company_name,
    TRIM(substring(raw_line, 93, 2)) AS industry_id,
    TRIM(substring(raw_line, 95, 4)) AS sp_rating,
    TRIM(substring(raw_line, 89, 4)) AS status,
    try_to_date(substring(raw_line, 99, 8), 'yyyyMMdd') AS founding_date,
    TRIM(substring(raw_line, 348, 46)) AS ceo_name,
    TRIM(substring(raw_line, 107, 80)) AS address_line1,
    TRIM(substring(raw_line, 187, 80)) AS address_line2,
    TRIM(substring(raw_line, 267, 12)) AS postal_code,
    TRIM(substring(raw_line, 279, 25)) AS city,
    TRIM(substring(raw_line, 304, 20)) AS state_province,
    TRIM(substring(raw_line, 324, 24)) AS country,
    TRIM(substring(raw_line, 394, 150)) AS description,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_finwire
WHERE _batch_id = {batch_id}
  AND substring(raw_line, 16, 3) = 'CMP'
  AND length(raw_line) >= 394
""")
