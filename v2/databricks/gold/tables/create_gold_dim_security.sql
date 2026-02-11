-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_security
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_dim_security: Security dimension (from FINWIRE SEC)
CREATE TABLE IF NOT EXISTS gold_dim_security (
    sk_security_id STRING NOT NULL,  -- Natural key (Symbol)
    security_id STRING NOT NULL,  -- Same as symbol
    symbol STRING NOT NULL,
    issue_type STRING,
    status STRING,
    name STRING,
    exchange_id STRING,
    shares_outstanding BIGINT,
    first_trade_date DATE,
    first_trade_exchange STRING,
    dividend DOUBLE,
    company_id STRING,  -- Reference to DimCompany
    is_current BOOLEAN NOT NULL,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
