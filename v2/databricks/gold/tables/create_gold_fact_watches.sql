-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_fact_watches
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_fact_watches: Watches fact
CREATE TABLE IF NOT EXISTS gold_fact_watches (
    sk_customer_id BIGINT NOT NULL,
    sk_security_id STRING NOT NULL,
    customer_id BIGINT NOT NULL,
    symbol STRING NOT NULL,
    watch_date TIMESTAMP NOT NULL,
    watch_action STRING,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
