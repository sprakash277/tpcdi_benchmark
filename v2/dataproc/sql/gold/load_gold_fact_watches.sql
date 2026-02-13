DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_fact_watches;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_fact_watches USING DELTA AS
SELECT 
    dc.sk_customer_id,
    ds.sk_security_id,
    swh.w_c_id AS customer_id,
    swh.w_s_symb AS symbol,
    swh.w_dts AS watch_date,
    swh.w_action AS watch_action,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_watch_history swh
-- 1. JOIN TO CUSTOMER: equi-join on customer_id (no point-in-time to avoid expensive date-range join / hang)
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc 
    ON CAST(swh.w_c_id AS BIGINT) = CAST(dc.customer_id AS BIGINT)
-- 2. JOIN TO SECURITY: UPPER(TRIM) on symbol
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds 
    ON UPPER(TRIM(swh.w_s_symb)) = UPPER(TRIM(ds.symbol))
WHERE swh.batch_id = __BATCH_ID__
  AND swh.is_current = true
