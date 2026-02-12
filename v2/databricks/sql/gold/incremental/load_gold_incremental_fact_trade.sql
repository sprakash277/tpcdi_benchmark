-- TPC-DI v2: Gold incremental - gold_fact_trade (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_fact_trade
SELECT 
    st.trade_id AS sk_trade_id,
    dd.sk_date_id,
    dt.sk_time_id,
    COALESCE(dc.sk_customer_id, -1) AS sk_customer_id,
    COALESCE(da.sk_account_id, -1) AS sk_account_id,
    COALESCE(ds.sk_security_id, 'UNKNOWN') AS sk_security_id,
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
    CASE WHEN dc.sk_customer_id IS NULL OR da.sk_account_id IS NULL THEN true ELSE false END AS late_arriving_flag,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_trades st
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
LEFT JOIN __CATALOG__.__SCHEMA__.gold_dim_time dt ON HOUR(st.trade_dts) = dt.hour_id
LEFT JOIN __CATALOG__.__SCHEMA__.gold_dim_account da ON st.account_id = da.account_id
LEFT JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc ON da.customer_id = dc.customer_id
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds ON st.symbol = ds.symbol
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_trade_type dtt ON st.trade_type_id = dtt.trade_type_id
WHERE st.batch_id = __BATCH_ID__
  AND st.is_current = true
  AND st.record_type IN ('I', 'U');
