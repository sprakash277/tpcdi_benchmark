-- ============================================================================
-- TPC-DI v2: Bronze Layer - Incremental Load (Batch 2+)
-- ============================================================================
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__, __RAW_DATA_PATH__
-- ============================================================================

USE CATALOG __CATALOG__;
USE SCHEMA __SCHEMA__;

-- Load Customer.txt (incremental)
INSERT INTO __CATALOG__.__SCHEMA__.bronze_customer (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Customer.txt' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch__BATCH_ID__/Customer.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load Account.txt (incremental)
INSERT INTO __CATALOG__.__SCHEMA__.bronze_account (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Account.txt' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch__BATCH_ID__/Account.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load Trade.txt (incremental)
INSERT INTO __CATALOG__.__SCHEMA__.bronze_trade (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Trade.txt' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch__BATCH_ID__/Trade.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load DailyMarket.txt (incremental)
INSERT INTO __CATALOG__.__SCHEMA__.bronze_daily_market (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'DailyMarket.txt' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch__BATCH_ID__/DailyMarket.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load CashTransaction.txt (incremental)
INSERT INTO __CATALOG__.__SCHEMA__.bronze_cash_transaction (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'CashTransaction.txt' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch__BATCH_ID__/CashTransaction.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load HoldingHistory.txt (incremental)
INSERT INTO __CATALOG__.__SCHEMA__.bronze_holding_history (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'HoldingHistory.txt' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch__BATCH_ID__/HoldingHistory.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load WatchHistory.txt (incremental)
INSERT INTO __CATALOG__.__SCHEMA__.bronze_watch_history (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'WatchHistory.txt' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch__BATCH_ID__/WatchHistory.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load Prospect.csv (incremental)
INSERT INTO __CATALOG__.__SCHEMA__.bronze_prospect (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Prospect.csv' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch__BATCH_ID__/Prospect.csv', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
