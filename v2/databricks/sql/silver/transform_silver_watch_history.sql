CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_watch_history AS
SELECT 
    CONCAT(CAST(split(raw_line, '__PIPE__')[0] AS BIGINT), '|', split(raw_line, '__PIPE__')[1]) AS wh_key,
    CAST(split(raw_line, '__PIPE__')[0] AS BIGINT) AS w_c_id,
    split(raw_line, '__PIPE__')[1] AS w_s_symb,
    CAST(split(raw_line, '__PIPE__')[2] AS TIMESTAMP) AS w_dts,
    split(raw_line, '__PIPE__')[3] AS w_action,
    TRUE AS is_current,
    CAST(split(raw_line, '__PIPE__')[2] AS TIMESTAMP) AS effective_date,
    NULL AS end_date,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM __CATALOG__.__SCHEMA__.bronze_watch_history
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '__PIPE__')) = 4
