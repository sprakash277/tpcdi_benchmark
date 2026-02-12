CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_status_type AS
SELECT 
    split_part(raw_line, '|', 1) AS st_id,
    split_part(raw_line, '|', 2) AS st_name,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_status_type
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
