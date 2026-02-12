CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_industry AS
SELECT 
    split_part(raw_line, '|', 1) AS in_id,
    split_part(raw_line, '|', 2) AS in_name,
    split_part(raw_line, '|', 3) AS in_sc_id,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_industry
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
