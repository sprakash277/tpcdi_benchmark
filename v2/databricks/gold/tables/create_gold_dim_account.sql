-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_account
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_dim_account: Account dimension
CREATE TABLE IF NOT EXISTS gold_dim_account (
    sk_account_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,  -- Natural key
    broker_id BIGINT,
    customer_id BIGINT NOT NULL,
    account_name STRING,
    tax_status INT,
    status_id STRING,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
