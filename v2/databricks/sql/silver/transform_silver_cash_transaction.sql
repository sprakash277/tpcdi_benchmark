CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_cash_transaction AS
SELECT 
    CONCAT(CAST(split(raw_line, '__PIPE__')[0] AS BIGINT), '|', CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP)) AS ct_key,
    CAST(split(raw_line, '__PIPE__')[0] AS BIGINT) AS ct_ca_id,
    CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS ct_dts,
    CAST(split(raw_line, '__PIPE__')[2] AS DOUBLE) AS ct_amt,
    split(raw_line, '__PIPE__')[3] AS ct_name,
    TRUE AS is_current,
    CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS effective_date,
    NULL AS end_date,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM __CATALOG__.__SCHEMA__.bronze_cash_transaction
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '__PIPE__')) = 4
