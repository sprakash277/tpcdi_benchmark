-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_trade
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- bronze_trade: Pipe-delimited trade records
CREATE TABLE IF NOT EXISTS bronze_trade (
    raw_line STRING,                   -- Pipe-delimited (16 cols historical, 18 cols incremental)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
