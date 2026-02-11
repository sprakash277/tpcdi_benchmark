# Databricks notebook source
# Load gold_fact_cash_balances from silver_cash_transaction (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_fact_cash_balances AS
SELECT 
    dd.sk_date_id,
    da.sk_account_id,
    dc.sk_customer_id,
    sct.ct_ca_id AS account_id,
    SUM(sct.ct_amt) AS cash_balance,
    COUNT(*) AS transaction_count,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_cash_transaction sct
INNER JOIN {catalog}.{schema_name}.gold_dim_date dd ON DATE(sct.ct_dts) = dd.date_value
INNER JOIN {catalog}.{schema_name}.gold_dim_account da ON sct.ct_ca_id = da.account_id
INNER JOIN {catalog}.{schema_name}.gold_dim_customer dc ON da.customer_id = dc.customer_id
WHERE sct.batch_id = {batch_id}
  AND sct.is_current = true
GROUP BY dd.sk_date_id, da.sk_account_id, dc.sk_customer_id, sct.ct_ca_id
""")
