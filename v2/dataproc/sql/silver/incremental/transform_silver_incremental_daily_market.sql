-- TPC-DI v2: Silver incremental - silver_daily_market (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__
-- Delta on Dataproc (Hive catalog) may not support MERGE; use overwrite with (existing - source keys) UNION ALL source.

WITH source AS (
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
),
merged AS (
    SELECT target.dm_key, target.dm_date, target.dm_s_symb, target.dm_close, target.dm_high, target.dm_low, target.dm_vol, target.batch_id, target.load_timestamp
    FROM __CATALOG__.__SCHEMA__.silver_daily_market AS target
    WHERE NOT EXISTS (SELECT 1 FROM source s WHERE s.dm_key = target.dm_key)
    UNION ALL
    SELECT dm_key, dm_date, dm_s_symb, dm_close, dm_high, dm_low, dm_vol, batch_id, load_timestamp FROM source
)
INSERT OVERWRITE TABLE __CATALOG__.__SCHEMA__.silver_daily_market SELECT * FROM merged;
