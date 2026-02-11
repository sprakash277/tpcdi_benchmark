CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_trades AS
SELECT 
    CAST(split(raw_line, '__PIPE__')[0] AS BIGINT) AS trade_id,
    CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS trade_dts,
    split(raw_line, '__PIPE__')[2] AS status_id,
    split(raw_line, '__PIPE__')[3] AS trade_type_id,
    CAST(split(raw_line, '__PIPE__')[4] AS BOOLEAN) AS is_cash,
    split(raw_line, '__PIPE__')[5] AS symbol,
    CAST(split(raw_line, '__PIPE__')[6] AS INT) AS quantity,
    CAST(split(raw_line, '__PIPE__')[7] AS DOUBLE) AS bid_price,
    CAST(split(raw_line, '__PIPE__')[8] AS BIGINT) AS account_id,
    split(raw_line, '__PIPE__')[9] AS exec_name,
    CAST(split(raw_line, '__PIPE__')[10] AS DOUBLE) AS trade_price,
    CAST(split(raw_line, '__PIPE__')[11] AS DOUBLE) AS charge,
    CAST(split(raw_line, '__PIPE__')[12] AS DOUBLE) AS commission,
    CAST(split(raw_line, '__PIPE__')[13] AS DOUBLE) AS tax,
    TRUE AS is_current,
    CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS effective_date,
    NULL AS end_date,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM __CATALOG__.__SCHEMA__.bronze_trade
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '__PIPE__')) = 16
