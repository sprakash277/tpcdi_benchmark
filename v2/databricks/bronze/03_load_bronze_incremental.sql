-- ============================================================================
-- TPC-DI v2: Bronze Layer - Incremental Load (Batch 2+)
-- ============================================================================
-- Loads raw files from Batch{N}/ directory into Bronze tables
-- Uses APPEND mode to add incremental data
-- ============================================================================

-- Set variables (adjust paths and batch_id as needed)
-- SET var.raw_data_path = '/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10';
-- SET var.batch_id = 2;  -- Change for Batch 3, 4, etc.

-- ============================================================================
-- Brokerage Data (Batch 2+: Pipe-delimited)
-- ============================================================================

-- Load Customer.txt (incremental)
INSERT INTO bronze_customer (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    ${var.batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Customer.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/Customer.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load Account.txt (incremental)
INSERT INTO bronze_account (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    ${var.batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Account.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/Account.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- ============================================================================
-- Transaction Data (Batch 2+: All batches)
-- ============================================================================

-- Load Trade.txt (incremental)
INSERT INTO bronze_trade (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    ${var.batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Trade.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/Trade.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load DailyMarket.txt (incremental)
INSERT INTO bronze_daily_market (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    ${var.batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'DailyMarket.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/DailyMarket.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load CashTransaction.txt (incremental)
INSERT INTO bronze_cash_transaction (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    ${var.batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'CashTransaction.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/CashTransaction.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load HoldingHistory.txt (incremental)
INSERT INTO bronze_holding_history (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    ${var.batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'HoldingHistory.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/HoldingHistory.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load WatchHistory.txt (incremental)
INSERT INTO bronze_watch_history (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    ${var.batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'WatchHistory.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/WatchHistory.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- ============================================================================
-- Other Sources (Batch 2+: Prospect only)
-- ============================================================================

-- Load Prospect.csv (incremental)
INSERT INTO bronze_prospect (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    ${var.batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Prospect.csv' AS _source_file
FROM read_files('${var.raw_data_path}/Batch${var.batch_id}/Prospect.csv', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
