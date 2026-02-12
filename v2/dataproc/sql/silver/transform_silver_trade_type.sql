DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.silver_trade_type;
CREATE TABLE __CATALOG__.__SCHEMA__.silver_trade_type AS
SELECT 
    split_part(raw_line, '|', 1) AS tt_id,
    split_part(raw_line, '|', 2) AS tt_name,
    CAST(split_part(raw_line, '|', 3) AS BOOLEAN) AS tt_is_sell,
    CAST(split_part(raw_line, '|', 4) AS BOOLEAN) AS tt_is_mrkt,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_trade_type
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
