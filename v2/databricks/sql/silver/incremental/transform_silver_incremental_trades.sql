-- TPC-DI v2: Silver incremental - silver_trades (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

WITH incoming_trades AS (
    SELECT 
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS trade_id,
        try_cast(split_part(raw_line, '|', 4) AS TIMESTAMP) AS trade_dts,
        split_part(raw_line, '|', 5) AS status_id,
        split_part(raw_line, '|', 6) AS trade_type_id,
        CASE WHEN split_part(raw_line, '|', 7) = '1' THEN true ELSE false END AS is_cash,
        split_part(raw_line, '|', 8) AS symbol,
        try_cast(split_part(raw_line, '|', 9) AS INT) AS quantity,
        try_cast(split_part(raw_line, '|', 10) AS DOUBLE) AS bid_price,
        try_cast(split_part(raw_line, '|', 11) AS BIGINT) AS account_id,
        split_part(raw_line, '|', 12) AS exec_name,
        try_cast(split_part(raw_line, '|', 13) AS DOUBLE) AS trade_price,
        try_cast(split_part(raw_line, '|', 14) AS DOUBLE) AS charge,
        try_cast(split_part(raw_line, '|', 15) AS DOUBLE) AS commission,
        try_cast(split_part(raw_line, '|', 16) AS DOUBLE) AS tax,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id
    FROM __CATALOG__.__SCHEMA__.bronze_trade
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) >= 15
)
INSERT INTO __CATALOG__.__SCHEMA__.silver_trades
SELECT 
    trade_id,
    trade_dts,
    status_id,
    trade_type_id,
    is_cash,
    symbol,
    quantity,
    bid_price,
    account_id,
    exec_name,
    trade_price,
    charge,
    commission,
    tax,
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    CAST(NULL AS TIMESTAMP) AS end_date,
    batch_id,
    current_timestamp() AS load_timestamp,
    cdc_flag AS record_type
FROM incoming_trades
WHERE cdc_flag IN ('I', 'U');
