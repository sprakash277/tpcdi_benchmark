-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_daily_market
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- bronze_daily_market: Pipe-delimited market data
CREATE TABLE IF NOT EXISTS bronze_daily_market (
    raw_line STRING,                   -- Pipe-delimited (6 cols historical, 8 cols incremental)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
