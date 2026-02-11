-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_company
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- gold_dim_company: Company dimension (from FINWIRE CMP)
CREATE TABLE IF NOT EXISTS gold_dim_company (
    sk_company_id BIGINT NOT NULL,
    company_id STRING NOT NULL,  -- Natural key (CIK)
    company_name STRING,
    industry_id STRING,
    sector STRING,  -- Derived from industry
    status STRING,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state_prov STRING,
    country STRING,
    description STRING,
    founding_date DATE,
    ceo_name STRING,
    is_current BOOLEAN NOT NULL,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
