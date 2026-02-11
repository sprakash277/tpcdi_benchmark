-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_fact_trade
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_fact_trade: Trade fact table
CREATE TABLE IF NOT EXISTS gold_fact_trade (
    sk_trade_id BIGINT NOT NULL,
    sk_date_id INT NOT NULL,
    sk_time_id INT,
    sk_customer_id BIGINT NOT NULL,
    sk_account_id BIGINT NOT NULL,
    sk_security_id STRING NOT NULL,
    sk_trade_type_id STRING NOT NULL,
    trade_id BIGINT NOT NULL,
    trade_dts TIMESTAMP NOT NULL,
    trade_price DOUBLE,
    trade_quantity INT,
    trade_amount DOUBLE,
    commission DOUBLE,
    charge DOUBLE,
    tax DOUBLE,
    status_id STRING,
    is_cash BOOLEAN,
    exec_name STRING,
    batch_id INT NOT NULL,
    late_arriving_flag BOOLEAN,  -- True if trade arrived before account/customer
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
