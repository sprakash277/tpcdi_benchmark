-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_fact_market_history
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_fact_market_history: Market history fact
CREATE TABLE IF NOT EXISTS gold_fact_market_history (
    sk_date_id INT NOT NULL,
    sk_security_id STRING NOT NULL,
    sk_company_id BIGINT,
    market_date DATE NOT NULL,
    symbol STRING NOT NULL,
    close_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    volume BIGINT,
    batch_id INT NOT NULL,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
