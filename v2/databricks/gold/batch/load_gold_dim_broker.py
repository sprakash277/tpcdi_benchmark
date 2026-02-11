# Databricks notebook source
# Load gold_dim_broker from bronze_hr (parse HR.csv for brokers)
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_dim_broker AS
SELECT 
    monotonically_increasing_id() AS sk_broker_id,
    CAST(employee_id AS BIGINT) AS broker_id,
    CONCAT(first_name, ' ', last_name) AS broker_name,
    branch AS branch,
    office AS office,
    phone AS phone,
    TRUE AS is_current,
    current_timestamp() AS etl_timestamp
FROM (
    SELECT DISTINCT
        split(raw_line, ',')[0] AS employee_id,
        split(raw_line, ',')[1] AS manager_id,
        split(raw_line, ',')[2] AS first_name,
        split(raw_line, ',')[3] AS last_name,
        split(raw_line, ',')[4] AS branch,
        split(raw_line, ',')[5] AS office,
        split(raw_line, ',')[6] AS phone,
        split(raw_line, ',')[7] AS job_code
    FROM {catalog}.{schema_name}.bronze_hr
    WHERE _batch_id = {batch_id}
      AND raw_line IS NOT NULL
      AND split(raw_line, ',')[7] LIKE '%BROKER%'
) AS brokers
""")
