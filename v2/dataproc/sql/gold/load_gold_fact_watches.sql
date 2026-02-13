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
-- 1. JOIN TO CUSTOMER: Ensure types match and use Point-in-Time logic
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc 
    ON CAST(swh.w_c_id AS BIGINT) = CAST(dc.customer_id AS BIGINT)
    AND swh.w_dts >= dc.start_date 
    AND swh.w_dts < dc.end_date
-- 2. JOIN TO SECURITY: Use UPPER() for symbol and Point-in-Time logic
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds 
    ON UPPER(TRIM(swh.w_s_symb)) = UPPER(TRIM(ds.symbol))
    AND swh.w_dts >= ds.start_date 
    AND swh.w_dts < ds.end_date
WHERE swh.batch_id = __BATCH_ID__
  AND swh.is_current = true
