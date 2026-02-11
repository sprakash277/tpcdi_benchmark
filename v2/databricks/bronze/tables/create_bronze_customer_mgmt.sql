-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_customer_mgmt
-- ============================================================================
-- Set catalog and schema (variables set by workflow)
-- -- USE CATALOG ${var.catalog};
-- -- USE SCHEMA ${var.schema};



-- bronze_customer_mgmt: XML file from Batch 1 only
CREATE TABLE IF NOT EXISTS bronze_customer_mgmt (
    raw_xml STRING,                    -- Raw XML content
    _batch_id INT,                     -- Batch number (1, 2, 3)
    _load_timestamp TIMESTAMP,         -- When ingested
    _source_file STRING                -- Source file name
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
