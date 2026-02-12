CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_watch_history AS
SELECT 
    CONCAT(CAST(split_part(raw_line, '|', 1) AS BIGINT), '|', split_part(raw_line, '|', 2)) AS wh_key,
    CAST(split_part(raw_line, '|', 1) AS BIGINT) AS w_c_id,
    split_part(raw_line, '|', 2) AS w_s_symb,
    CAST(split_part(raw_line, '|', 3) AS TIMESTAMP) AS w_dts,
    split_part(raw_line, '|', 4) AS w_action,
    TRUE AS is_current,
    CAST(split_part(raw_line, '|', 3) AS TIMESTAMP) AS effective_date,
    NULL AS end_date,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM __CATALOG__.__SCHEMA__.bronze_watch_history
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '|')) = 4
