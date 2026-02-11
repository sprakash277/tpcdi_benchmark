-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_cash_transaction
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_cash_transaction: Cash transactions
CREATE TABLE IF NOT EXISTS silver_cash_transaction (
    ct_key STRING NOT NULL,  -- Composite: ct_ca_id + ct_dts
    ct_ca_id BIGINT NOT NULL,  -- Account ID
    ct_dts TIMESTAMP NOT NULL,
    ct_amt DOUBLE,
    ct_name STRING,
    
    -- SCD Type 2 Columns
    is_current BOOLEAN NOT NULL,
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    
    -- Metadata
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    record_type STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
