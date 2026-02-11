-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_time
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- bronze_time: Time dimension reference
CREATE TABLE IF NOT EXISTS bronze_time (
    raw_line STRING,                   -- Pipe-delimited (10 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
