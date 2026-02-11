-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_hr
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- bronze_hr: HR data (CSV, Batch 1 only)
CREATE TABLE IF NOT EXISTS bronze_hr (
    raw_line STRING,                   -- Comma-delimited (9 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
