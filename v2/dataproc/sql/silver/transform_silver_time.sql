DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.silver_time;
CREATE TABLE __CATALOG__.__SCHEMA__.silver_time AS
SELECT 
    CAST(split_part(raw_line, '|', 1) AS INT) AS sk_time_id,
    split_part(raw_line, '|', 2) AS time_value,
    CAST(split_part(raw_line, '|', 3) AS INT) AS hour_id,
    split_part(raw_line, '|', 4) AS hour_desc,
    CAST(split_part(raw_line, '|', 5) AS INT) AS minute_id,
    split_part(raw_line, '|', 6) AS minute_desc,
    CAST(split_part(raw_line, '|', 7) AS INT) AS second_id,
    split_part(raw_line, '|', 8) AS second_desc,
    CAST(split_part(raw_line, '|', 9) AS BOOLEAN) AS market_hours_flag,
    CAST(split_part(raw_line, '|', 10) AS BOOLEAN) AS office_hours_flag,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_time
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
