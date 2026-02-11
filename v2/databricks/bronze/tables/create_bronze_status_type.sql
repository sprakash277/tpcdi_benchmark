-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_status_type
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- bronze_status_type: Status type reference
CREATE TABLE IF NOT EXISTS bronze_status_type (
    raw_line STRING,                   -- Pipe-delimited (2 columns: ST_ID|ST_NAME)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
