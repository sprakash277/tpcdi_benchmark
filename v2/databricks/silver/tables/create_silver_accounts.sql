-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_accounts
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_accounts: Account dimension with SCD Type 2
CREATE TABLE IF NOT EXISTS silver_accounts (
    -- Business Key
    account_id BIGINT NOT NULL,
    
    -- Attributes
    broker_id BIGINT,
    customer_id BIGINT NOT NULL,
    account_name STRING,
    tax_status INT,
    status_id STRING,
    
    -- SCD Type 2 Columns
    is_current BOOLEAN NOT NULL,
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    
    -- Metadata
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    record_type STRING  -- I=Insert, U=Update, D=Delete
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
