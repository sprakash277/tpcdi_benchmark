-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_prospect
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- bronze_prospect: Prospect data (CSV, all batches)
CREATE TABLE IF NOT EXISTS bronze_prospect (
    raw_line STRING,                   -- Comma-delimited (23 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
