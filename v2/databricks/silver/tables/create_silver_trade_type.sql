-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_trade_type
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- silver_trade_type: Trade type reference
CREATE TABLE IF NOT EXISTS silver_trade_type (
    tt_id STRING NOT NULL,
    tt_name STRING,
    tt_is_sell BOOLEAN,
    tt_is_mrkt BOOLEAN,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
