CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_time AS
SELECT 
    CAST(split(raw_line, '__PIPE__')[0] AS INT) AS sk_time_id,
    split(raw_line, '__PIPE__')[1] AS time_value,
    CAST(split(raw_line, '__PIPE__')[2] AS INT) AS hour_id,
    split(raw_line, '__PIPE__')[3] AS hour_desc,
    CAST(split(raw_line, '__PIPE__')[4] AS INT) AS minute_id,
    split(raw_line, '__PIPE__')[5] AS minute_desc,
    CAST(split(raw_line, '__PIPE__')[6] AS INT) AS second_id,
    split(raw_line, '__PIPE__')[7] AS second_desc,
    CAST(split(raw_line, '__PIPE__')[8] AS BOOLEAN) AS market_hours_flag,
    CAST(split(raw_line, '__PIPE__')[9] AS BOOLEAN) AS office_hours_flag,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_time
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
