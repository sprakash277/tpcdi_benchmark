# Databricks notebook source
# Load gold_dim_industry from silver_industry (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_dim_industry AS
SELECT 
    in_id AS sk_industry_id,
    in_id AS industry_id,
    in_name AS industry_name,
    in_sc_id AS sector_id,
    NULL AS sector_name,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_industry
WHERE batch_id = {batch_id}
""")
