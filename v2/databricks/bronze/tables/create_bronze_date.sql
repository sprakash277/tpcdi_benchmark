-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_date
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- bronze_date: Date dimension reference
CREATE TABLE IF NOT EXISTS bronze_date (
    raw_line STRING,                   -- Pipe-delimited (18 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
