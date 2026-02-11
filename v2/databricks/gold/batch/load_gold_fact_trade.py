# Databricks notebook source
# Load gold_fact_trade from silver_trades + dimensions (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

sql = f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_fact_trade AS
SELECT 
    st.trade_id AS sk_trade_id,
    dd.sk_date_id,
    dt.sk_time_id,
    dc.sk_customer_id,
    da.sk_account_id,
    ds.sk_security_id,
    dtt.sk_trade_type_id,
    st.trade_id,
    st.trade_dts,
    st.trade_price,
    st.quantity AS trade_quantity,
    st.trade_price * st.quantity AS trade_amount,
    st.commission,
    st.charge,
    st.tax,
    st.status_id,
    st.is_cash,
    st.exec_name,
    st.batch_id,
    FALSE AS late_arriving_flag,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_trades st
INNER JOIN {catalog}.{schema_name}.gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
LEFT JOIN {catalog}.{schema_name}.gold_dim_time dt ON HOUR(st.trade_dts) = dt.hour_id
INNER JOIN {catalog}.{schema_name}.gold_dim_account da ON st.account_id = da.account_id
INNER JOIN {catalog}.{schema_name}.gold_dim_customer dc ON da.customer_id = dc.customer_id
INNER JOIN {catalog}.{schema_name}.gold_dim_security ds ON st.symbol = ds.symbol
INNER JOIN {catalog}.{schema_name}.gold_dim_trade_type dtt ON st.trade_type_id = dtt.trade_type_id
WHERE st.batch_id = {batch_id}
  AND st.is_current = true
"""
spark.sql(sql)
