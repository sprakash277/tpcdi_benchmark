CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.bronze_daily_market AS
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'DailyMarket.txt' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch1/DailyMarket.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != ''
