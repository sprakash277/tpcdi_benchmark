-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_fact_holdings
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_fact_holdings: Holdings fact
CREATE TABLE IF NOT EXISTS gold_fact_holdings (
    sk_date_id INT NOT NULL,
    sk_account_id BIGINT NOT NULL,
    sk_security_id STRING NOT NULL,
    account_id BIGINT NOT NULL,
    symbol STRING NOT NULL,
    quantity BIGINT,
    purchase_price DOUBLE,
    purchase_date DATE,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
