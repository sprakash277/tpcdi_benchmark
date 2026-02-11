-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_status_type
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_dim_status_type: Status type reference
CREATE TABLE IF NOT EXISTS gold_dim_status_type (
    sk_status_type_id STRING NOT NULL,
    status_type_id STRING NOT NULL,
    status_type_code STRING NOT NULL,  -- Same as status_type_id
    status_type_name STRING,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
