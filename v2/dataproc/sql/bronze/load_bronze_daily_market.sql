CREATE OR REPLACE TABLE __DATABASE__.bronze_daily_market AS
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'DailyMarket.txt' AS _source_file
FROM _tmp_bronze_daily_market
WHERE value IS NOT NULL AND value != ''
