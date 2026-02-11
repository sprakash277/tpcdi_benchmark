CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_daily_market AS
SELECT 
    CONCAT(CAST(split(raw_line, '__PIPE__')[0] AS DATE), '|', split(raw_line, '__PIPE__')[1]) AS dm_key,
    CAST(split(raw_line, '__PIPE__')[0] AS DATE) AS dm_date,
    split(raw_line, '__PIPE__')[1] AS dm_s_symb,
    CAST(split(raw_line, '__PIPE__')[2] AS DOUBLE) AS dm_close,
    CAST(split(raw_line, '__PIPE__')[3] AS DOUBLE) AS dm_high,
    CAST(split(raw_line, '__PIPE__')[4] AS DOUBLE) AS dm_low,
    CAST(split(raw_line, '__PIPE__')[5] AS BIGINT) AS dm_vol,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_daily_market
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '__PIPE__')) = 6
