CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_trades AS
SELECT 
    CAST(split_part(raw_line, '|', 1) AS BIGINT) AS trade_id,
    CAST(split_part(raw_line, '|', 2) AS TIMESTAMP) AS trade_dts,
    split_part(raw_line, '|', 3) AS status_id,
    split_part(raw_line, '|', 4) AS trade_type_id,
    CAST(split_part(raw_line, '|', 5) AS BOOLEAN) AS is_cash,
    split_part(raw_line, '|', 6) AS symbol,
    CAST(split_part(raw_line, '|', 7) AS INT) AS quantity,
    CAST(split_part(raw_line, '|', 8) AS DOUBLE) AS bid_price,
    CAST(split_part(raw_line, '|', 9) AS BIGINT) AS account_id,
    split_part(raw_line, '|', 10) AS exec_name,
    CAST(split_part(raw_line, '|', 11) AS DOUBLE) AS trade_price,
    CAST(split_part(raw_line, '|', 12) AS DOUBLE) AS charge,
    CAST(split_part(raw_line, '|', 13) AS DOUBLE) AS commission,
    CAST(split_part(raw_line, '|', 14) AS DOUBLE) AS tax,
    TRUE AS is_current,
    CAST(split_part(raw_line, '|', 2) AS TIMESTAMP) AS effective_date,
    NULL AS end_date,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM __CATALOG__.__SCHEMA__.bronze_trade
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '|')) = 16
