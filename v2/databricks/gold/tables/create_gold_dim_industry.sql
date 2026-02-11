-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_industry
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- gold_dim_industry: Industry reference
CREATE TABLE IF NOT EXISTS gold_dim_industry (
    sk_industry_id STRING NOT NULL,
    industry_id STRING NOT NULL,
    industry_name STRING,
    sector_id STRING,
    sector_name STRING,  -- Derived or lookup
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
