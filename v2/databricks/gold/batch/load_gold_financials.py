# Databricks notebook source
# Load gold_financials from silver_financials
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_financials AS
SELECT 
    co_name_or_cik,
    year,
    quarter,
    qtr_start_date,
    posting_date,
    revenue,
    earnings,
    eps,
    diluted_eps,
    margin,
    inventory,
    assets,
    liabilities,
    sh_out,
    diluted_sh_out,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_financials
WHERE batch_id = {batch_id}
""")
