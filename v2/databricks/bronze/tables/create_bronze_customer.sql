-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_customer
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- bronze_customer: Pipe-delimited from Batch 2+ only
CREATE TABLE IF NOT EXISTS bronze_customer (
    raw_line STRING,                   -- Raw pipe-delimited line
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
