-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_trade
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- gold_dim_trade: Trade dimension (per spec)
CREATE TABLE IF NOT EXISTS gold_dim_trade (
    sk_trade_id BIGINT NOT NULL,
    trade_id BIGINT NOT NULL,  -- Natural key
    trade_dts TIMESTAMP NOT NULL,
    trade_status STRING,
    trade_type STRING,
    is_cash BOOLEAN,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
