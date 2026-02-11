-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_finwire
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- bronze_finwire: Fixed-width records (CMP, SEC, FIN)
CREATE TABLE IF NOT EXISTS bronze_finwire (
    raw_line STRING,                   -- Fixed-width string (364 chars)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING                -- e.g., FINWIRE1967Q1.txt
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
