-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_industry
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- silver_industry: Industry reference
CREATE TABLE IF NOT EXISTS silver_industry (
    in_id STRING NOT NULL,
    in_name STRING,
    in_sc_id STRING,  -- Sector ID
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
