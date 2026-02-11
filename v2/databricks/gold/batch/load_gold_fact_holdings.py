# Databricks notebook source
# Load gold_fact_holdings from silver_holding_history + silver_trades (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

sql = f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_fact_holdings AS
SELECT 
    dd.sk_date_id,
    da.sk_account_id,
    ds.sk_security_id,
    st.account_id,
    st.symbol,
    shh.hh_after_qty AS quantity,
    st.trade_price AS purchase_price,
    DATE(st.trade_dts) AS purchase_date,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_holding_history shh
INNER JOIN {catalog}.{schema_name}.silver_trades st ON shh.hh_t_id = st.trade_id
INNER JOIN {catalog}.{schema_name}.gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
INNER JOIN {catalog}.{schema_name}.gold_dim_account da ON st.account_id = da.account_id
INNER JOIN {catalog}.{schema_name}.gold_dim_security ds ON st.symbol = ds.symbol
WHERE shh.batch_id = {batch_id}
  AND shh.is_current = true
  AND st.is_current = true
"""
spark.sql(sql)
