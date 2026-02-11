-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_trade_type
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_dim_trade_type: Trade type reference
CREATE TABLE IF NOT EXISTS gold_dim_trade_type (
    sk_trade_type_id STRING NOT NULL,
    trade_type_id STRING NOT NULL,
    trade_type_code STRING NOT NULL,  -- Same as trade_type_id
    trade_type_name STRING,
    is_sell BOOLEAN,
    is_market BOOLEAN,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
