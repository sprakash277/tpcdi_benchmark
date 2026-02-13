DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_fact_trade;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_fact_trade USING DELTA AS
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
FROM __CATALOG__.__SCHEMA__.silver_trades st
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_date dd
    ON CAST(st.trade_dts AS DATE) = CAST(dd.date_value AS DATE)
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_time dt
    ON date_format(st.trade_dts, 'HH:mm:ss') = dt.time_value
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_account da
    ON TRIM(CAST(st.account_id AS STRING)) = TRIM(CAST(da.account_id AS STRING))
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc
    ON TRIM(CAST(da.customer_id AS STRING)) = TRIM(CAST(dc.customer_id AS STRING))
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds
    ON TRIM(CAST(st.symbol AS STRING)) = TRIM(CAST(ds.symbol AS STRING))
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_trade_type dtt
    ON TRIM(CAST(st.trade_type_id AS STRING)) = TRIM(CAST(dtt.trade_type_id AS STRING))
WHERE st.batch_id = __BATCH_ID__
  AND st.is_current = true
