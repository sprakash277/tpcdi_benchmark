-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_status_type
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_status_type: Status type reference
CREATE TABLE IF NOT EXISTS silver_status_type (
    st_id STRING NOT NULL,
    st_name STRING,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
