CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_holding_history AS
SELECT 
    CAST(split(raw_line, '__PIPE__')[0] AS BIGINT) AS hh_h_t_id,
    CAST(split(raw_line, '__PIPE__')[1] AS BIGINT) AS hh_t_id,
    CAST(split(raw_line, '__PIPE__')[2] AS INT) AS hh_before_qty,
    CAST(split(raw_line, '__PIPE__')[3] AS INT) AS hh_after_qty,
    TRUE AS is_current,
    current_timestamp() AS effective_date,
    NULL AS end_date,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM __CATALOG__.__SCHEMA__.bronze_holding_history
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '__PIPE__')) = 4
