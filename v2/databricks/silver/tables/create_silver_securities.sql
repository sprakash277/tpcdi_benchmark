-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_securities
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_securities: Security records from FINWIRE (SEC)
CREATE TABLE IF NOT EXISTS silver_securities (
    symbol STRING NOT NULL,
    issue_type STRING,
    status STRING,
    name STRING,
    ex_id STRING,
    sh_out BIGINT,
    first_trade_date DATE,
    first_trade_exchg STRING,
    dividend DOUBLE,
    co_name_or_cik STRING,  -- Company reference
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
