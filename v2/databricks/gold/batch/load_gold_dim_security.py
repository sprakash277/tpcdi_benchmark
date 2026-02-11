# Databricks notebook source
# Load gold_dim_security from silver_securities (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_dim_security AS
SELECT 
    ss.symbol AS sk_security_id,
    ss.symbol AS security_id,
    ss.symbol,
    ss.issue_type,
    ss.status,
    ss.name,
    ss.ex_id AS exchange_id,
    ss.sh_out AS shares_outstanding,
    ss.first_trade_date,
    ss.first_trade_exchg AS first_trade_exchange,
    ss.dividend,
    ss.co_name_or_cik AS company_id,
    TRUE AS is_current,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_securities ss
WHERE ss.batch_id = {batch_id}
""")
