DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_fact_watches;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_fact_watches AS
SELECT 
    dc.sk_customer_id,
    ds.sk_security_id,
    swh.w_c_id AS customer_id,
    swh.w_s_symb AS symbol,
    swh.w_dts AS watch_date,
    swh.w_action AS watch_action,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_watch_history swh
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc ON swh.w_c_id = dc.customer_id
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds ON swh.w_s_symb = ds.symbol
WHERE swh.batch_id = __BATCH_ID__
  AND swh.is_current = true
