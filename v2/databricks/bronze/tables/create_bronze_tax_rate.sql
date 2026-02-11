-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_tax_rate
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- bronze_tax_rate: Tax rate reference
CREATE TABLE IF NOT EXISTS bronze_tax_rate (
    raw_line STRING,                   -- Pipe-delimited (3 columns)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
