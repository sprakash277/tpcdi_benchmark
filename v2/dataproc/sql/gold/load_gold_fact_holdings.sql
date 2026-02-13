DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_fact_holdings;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_fact_holdings AS
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
FROM __CATALOG__.__SCHEMA__.silver_holding_history shh
INNER JOIN __CATALOG__.__SCHEMA__.silver_trades st ON shh.hh_t_id = st.trade_id
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_account da ON CAST(st.account_id AS STRING) = CAST(da.account_id AS STRING)
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds ON TRIM(CAST(st.symbol AS STRING)) = TRIM(CAST(ds.symbol AS STRING))
WHERE shh.batch_id = __BATCH_ID__
  AND shh.is_current = true
  AND st.is_current = true
