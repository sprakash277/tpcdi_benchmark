CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_daily_market AS
SELECT 
    -- Composite Key: Date + Symbol (Used for Upserts in Incremental Batches)
    CONCAT(CAST(split_part(raw_line, '|', 1) AS STRING), '|', split_part(raw_line, '|', 2)) AS dm_key,
    CAST(split_part(raw_line, '|', 1) AS DATE) AS dm_date,
    split_part(raw_line, '|', 2) AS dm_s_symb,
    CAST(split_part(raw_line, '|', 3) AS DOUBLE) AS dm_close,
    CAST(split_part(raw_line, '|', 4) AS DOUBLE) AS dm_high,
    CAST(split_part(raw_line, '|', 5) AS DOUBLE) AS dm_low,
    CAST(split_part(raw_line, '|', 6) AS BIGINT) AS dm_vol,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_daily_market
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  -- Must use double backslash to escape pipe in size(split())
  AND size(split(raw_line, '\\|')) = 6
