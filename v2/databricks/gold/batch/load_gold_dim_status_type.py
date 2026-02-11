# Databricks notebook source
# Load gold_dim_status_type from silver_status_type (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_dim_status_type AS
SELECT 
    st_id AS sk_status_type_id,
    st_id AS status_type_id,
    st_id AS status_type_code,
    st_name AS status_type_name,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_status_type
WHERE batch_id = {batch_id}
""")
