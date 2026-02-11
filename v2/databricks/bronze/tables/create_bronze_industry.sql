-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_industry
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- bronze_industry: Industry reference
CREATE TABLE IF NOT EXISTS bronze_industry (
    raw_line STRING,                   -- Pipe-delimited (3 columns: IN_ID|IN_NAME|IN_SC_ID)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
