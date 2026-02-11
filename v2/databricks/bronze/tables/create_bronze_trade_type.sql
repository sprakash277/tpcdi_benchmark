-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_trade_type
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- bronze_trade_type: Trade type reference
CREATE TABLE IF NOT EXISTS bronze_trade_type (
    raw_line STRING,                   -- Pipe-delimited (4 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
