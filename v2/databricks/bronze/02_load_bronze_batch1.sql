-- ============================================================================
-- TPC-DI v2: Bronze Layer - Batch 1 Load (Historical)
-- ============================================================================
-- Loads raw files from Batch1/ directory into Bronze tables
-- All data stored as STRING to ensure ingestion never fails
-- ============================================================================

-- Set variables (adjust paths as needed)
-- SET var.raw_data_path = '/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10';
-- SET var.batch_id = 1;

-- ============================================================================
-- Reference Data (Batch 1 only)
-- ============================================================================

-- Load Date.txt
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};


INSERT INTO bronze_date (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Date.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/Date.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load Time.txt
INSERT INTO bronze_time (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Time.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/Time.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load StatusType.txt
INSERT INTO bronze_status_type (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'StatusType.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/StatusType.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load TradeType.txt
INSERT INTO bronze_trade_type (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'TradeType.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/TradeType.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load Industry.txt
INSERT INTO bronze_industry (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Industry.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/Industry.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load TaxRate.txt
INSERT INTO bronze_tax_rate (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'TaxRate.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/TaxRate.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- ============================================================================
-- Brokerage Data (Batch 1: XML)
-- ============================================================================

-- Load CustomerMgmt.xml (XML file - use spark-xml or native XML reader)
INSERT INTO bronze_customer_mgmt (raw_xml, _batch_id, _load_timestamp, _source_file)
SELECT 
    _c0 AS raw_xml,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'CustomerMgmt.xml' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/CustomerMgmt.xml', format => 'xml', rowTag => 'Customer');

-- ============================================================================
-- Market Data (Batch 1: Fixed-Width FINWIRE)
-- ============================================================================

-- Load FINWIRE files (multiple files: FINWIRE1967Q1.txt, FINWIRE1967Q2.txt, etc.)
-- Note: Adjust pattern to match your FINWIRE files
INSERT INTO bronze_finwire (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    input_file_name() AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/FINWIRE*.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND length(value) >= 18;  -- Ensure minimum length for record type

-- ============================================================================
-- Transaction Data (Batch 1)
-- ============================================================================

-- Load Trade.txt
INSERT INTO bronze_trade (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Trade.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/Trade.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load DailyMarket.txt
INSERT INTO bronze_daily_market (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'DailyMarket.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/DailyMarket.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load CashTransaction.txt
INSERT INTO bronze_cash_transaction (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'CashTransaction.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/CashTransaction.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load HoldingHistory.txt
INSERT INTO bronze_holding_history (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'HoldingHistory.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/HoldingHistory.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load WatchHistory.txt
INSERT INTO bronze_watch_history (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'WatchHistory.txt' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/WatchHistory.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- ============================================================================
-- Other Sources (Batch 1)
-- ============================================================================

-- Load HR.csv
INSERT INTO bronze_hr (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'HR.csv' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/HR.csv', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';

-- Load Prospect.csv
INSERT INTO bronze_prospect (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    1 AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Prospect.csv' AS _source_file
FROM read_files('${var.raw_data_path}/Batch1/Prospect.csv', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
