-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_holding_history
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- silver_holding_history: Holding history
CREATE TABLE IF NOT EXISTS silver_holding_history (
    hh_h_t_id BIGINT NOT NULL,  -- Holding history trade ID
    hh_t_id BIGINT,  -- Trade ID (join to silver_trades)
    hh_before_qty INT,
    hh_after_qty INT,
    
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
