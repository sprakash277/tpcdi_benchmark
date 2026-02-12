-- TPC-DI v2: Silver incremental - silver_daily_market (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

MERGE INTO __CATALOG__.__SCHEMA__.silver_daily_market AS target
USING (
    SELECT 
        CONCAT(try_cast(split_part(raw_line, '|', 3) AS DATE), '|', split_part(raw_line, '|', 4)) AS dm_key,
        try_cast(split_part(raw_line, '|', 3) AS DATE) AS dm_date,
        split_part(raw_line, '|', 4) AS dm_s_symb,
        try_cast(split_part(raw_line, '|', 5) AS DOUBLE) AS dm_close,
        try_cast(split_part(raw_line, '|', 6) AS DOUBLE) AS dm_high,
        try_cast(split_part(raw_line, '|', 7) AS DOUBLE) AS dm_low,
        try_cast(split_part(raw_line, '|', 8) AS BIGINT) AS dm_vol,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM __CATALOG__.__SCHEMA__.bronze_daily_market
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 8
) AS source
ON target.dm_key = source.dm_key
WHEN MATCHED THEN UPDATE SET
    target.dm_close = source.dm_close,
    target.dm_high = source.dm_high,
    target.dm_low = source.dm_low,
    target.dm_vol = source.dm_vol,
    target.batch_id = source.batch_id,
    target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN INSERT *;
