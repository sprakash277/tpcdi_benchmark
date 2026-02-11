# Databricks notebook source
# Load gold_dim_account from silver_accounts
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_dim_account AS
SELECT 
    monotonically_increasing_id() AS sk_account_id,
    account_id,
    broker_id,
    customer_id,
    account_name,
    tax_status,
    status_id,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_accounts
WHERE is_current = true
  AND batch_id = {batch_id}
  AND account_id != -1
""")
