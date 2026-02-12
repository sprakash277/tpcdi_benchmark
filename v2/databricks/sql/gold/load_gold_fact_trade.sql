CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.gold_fact_trade AS
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
    ON CAST(st.trade_dts AS DATE) = dd.date_value
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_time dt
    ON date_format(st.trade_dts, 'HH:mm:ss') = dt.time_value
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_account da
    ON st.account_id = da.account_id
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc
    ON da.customer_id = dc.customer_id
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds
    ON st.symbol = ds.symbol
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_trade_type dtt
    ON st.trade_type_id = dtt.trade_type_id
WHERE st.batch_id = __BATCH_ID__
  AND st.is_current = true
