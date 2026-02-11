-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_trades
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_trades: Trade transactions with SCD Type 2
CREATE TABLE IF NOT EXISTS silver_trades (
    trade_id BIGINT NOT NULL,
    trade_dts TIMESTAMP NOT NULL,
    status_id STRING,
    trade_type_id STRING,
    is_cash BOOLEAN,
    symbol STRING,
    quantity INT,
    bid_price DOUBLE,
    account_id BIGINT NOT NULL,
    exec_name STRING,
    trade_price DOUBLE,
    charge DOUBLE,
    commission DOUBLE,
    tax DOUBLE,
    
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
