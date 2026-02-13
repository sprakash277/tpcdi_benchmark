DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_trade_type;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_trade_type USING DELTA AS
SELECT 
    tt_id AS sk_trade_type_id,
    tt_id AS trade_type_id,
    tt_id AS trade_type_code,
    tt_name AS trade_type_name,
    tt_is_sell AS is_sell,
    tt_is_mrkt AS is_market,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_trade_type
WHERE batch_id = __BATCH_ID__
