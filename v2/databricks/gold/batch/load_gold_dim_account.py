# Databricks notebook source
# Load gold_dim_account from silver_accounts (widgets set by orchestrator)
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
