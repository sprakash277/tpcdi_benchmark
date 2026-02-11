# Databricks notebook source
# Load gold_financials from silver_financials (widgets set by orchestrator)
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
