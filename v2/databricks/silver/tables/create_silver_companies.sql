-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_companies
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- silver_companies: Company records from FINWIRE (CMP)
CREATE TABLE IF NOT EXISTS silver_companies (
    sk_company_id BIGINT,
    company_id STRING NOT NULL,  -- CIK
    company_name STRING,
    industry_id STRING,
    sp_rating STRING,
    status STRING,
    founding_date DATE,
    ceo_name STRING,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state_province STRING,
    country STRING,
    description STRING,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
