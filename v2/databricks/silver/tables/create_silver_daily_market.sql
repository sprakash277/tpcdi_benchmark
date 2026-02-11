-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_daily_market
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_daily_market: Daily market data
CREATE TABLE IF NOT EXISTS silver_daily_market (
    dm_key STRING NOT NULL,  -- Composite: dm_date + dm_s_symb
    dm_date DATE NOT NULL,
    dm_s_symb STRING NOT NULL,
    dm_close DOUBLE,
    dm_high DOUBLE,
    dm_low DOUBLE,
    dm_vol BIGINT,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
