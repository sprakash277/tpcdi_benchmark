-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_holding_history
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- bronze_holding_history: Pipe-delimited holding history
CREATE TABLE IF NOT EXISTS bronze_holding_history (
    raw_line STRING,                   -- Pipe-delimited (4 cols historical, 6 cols incremental)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
