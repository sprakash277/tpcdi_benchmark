# Databricks notebook source
# Load gold_dim_industry from silver_industry
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

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
