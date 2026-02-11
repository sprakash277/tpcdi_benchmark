-- ============================================================================
-- TPC-DI v2: Bronze Layer - Dataproc
-- ============================================================================
-- Bronze Layer: Raw data ingestion with no transformations
-- All columns stored as STRING to ensure ingestion never fails
-- Metadata columns: _batch_id, _load_timestamp, _source_file
-- Storage: GCS (gs://bucket/path)
-- ============================================================================

-- Set database (adjust as needed)
-- CREATE DATABASE IF NOT EXISTS tpcdi_bronze;
-- USE tpcdi_bronze;

-- ============================================================================
-- Brokerage Data (Batch 1: XML, Batch 2+: Pipe-delimited)
-- ============================================================================

-- bronze_customer_mgmt: XML file from Batch 1 only
CREATE TABLE IF NOT EXISTS bronze_customer_mgmt (
    raw_xml STRING,                    -- Raw XML content
    _batch_id INT,                     -- Batch number (1, 2, 3)
    _load_timestamp TIMESTAMP,         -- When ingested
    _source_file STRING                -- Source file name
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_customer_mgmt'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_customer: Pipe-delimited from Batch 2+ only
CREATE TABLE IF NOT EXISTS bronze_customer (
    raw_line STRING,                   -- Raw pipe-delimited line
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_customer'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_account: Pipe-delimited from Batch 2+ only
CREATE TABLE IF NOT EXISTS bronze_account (
    raw_line STRING,                   -- Raw pipe-delimited line
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_account'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Market Data (Fixed-Width FINWIRE files - Batch 1 only)
-- ============================================================================

-- bronze_finwire: Fixed-width records (CMP, SEC, FIN)
CREATE TABLE IF NOT EXISTS bronze_finwire (
    raw_line STRING,                   -- Fixed-width string (364 chars)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING                -- e.g., FINWIRE1967Q1.txt
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_finwire'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Transaction Data (All Batches)
-- ============================================================================

-- bronze_trade: Pipe-delimited trade records
CREATE TABLE IF NOT EXISTS bronze_trade (
    raw_line STRING,                   -- Pipe-delimited (16 cols historical, 18 cols incremental)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_trade'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_daily_market: Pipe-delimited market data
CREATE TABLE IF NOT EXISTS bronze_daily_market (
    raw_line STRING,                   -- Pipe-delimited (6 cols historical, 8 cols incremental)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_daily_market'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_cash_transaction: Pipe-delimited cash transactions
CREATE TABLE IF NOT EXISTS bronze_cash_transaction (
    raw_line STRING,                   -- Pipe-delimited (4 cols historical, 6 cols incremental)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_cash_transaction'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_holding_history: Pipe-delimited holding history
CREATE TABLE IF NOT EXISTS bronze_holding_history (
    raw_line STRING,                   -- Pipe-delimited (4 cols historical, 6 cols incremental)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_holding_history'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_watch_history: Pipe-delimited watch list
CREATE TABLE IF NOT EXISTS bronze_watch_history (
    raw_line STRING,                   -- Pipe-delimited (4 cols historical, 6 cols incremental)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_watch_history'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Reference Data (Batch 1 only)
-- ============================================================================

-- bronze_date: Date dimension reference
CREATE TABLE IF NOT EXISTS bronze_date (
    raw_line STRING,                   -- Pipe-delimited (18 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_date'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_time: Time dimension reference
CREATE TABLE IF NOT EXISTS bronze_time (
    raw_line STRING,                   -- Pipe-delimited (10 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_time'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_status_type: Status type reference
CREATE TABLE IF NOT EXISTS bronze_status_type (
    raw_line STRING,                   -- Pipe-delimited (2 columns: ST_ID|ST_NAME)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_status_type'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_trade_type: Trade type reference
CREATE TABLE IF NOT EXISTS bronze_trade_type (
    raw_line STRING,                   -- Pipe-delimited (4 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_trade_type'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_industry: Industry reference
CREATE TABLE IF NOT EXISTS bronze_industry (
    raw_line STRING,                   -- Pipe-delimited (3 columns: IN_ID|IN_NAME|IN_SC_ID)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_industry'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_tax_rate: Tax rate reference
CREATE TABLE IF NOT EXISTS bronze_tax_rate (
    raw_line STRING,                   -- Pipe-delimited (3 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_tax_rate'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Other Sources
-- ============================================================================

-- bronze_hr: HR data (CSV, Batch 1 only)
CREATE TABLE IF NOT EXISTS bronze_hr (
    raw_line STRING,                   -- Comma-delimited (9 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_hr'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- bronze_prospect: Prospect data (CSV, all batches)
CREATE TABLE IF NOT EXISTS bronze_prospect (
    raw_line STRING,                   -- Comma-delimited (23 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/bronze/bronze_prospect'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
