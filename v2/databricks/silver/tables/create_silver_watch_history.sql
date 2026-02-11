-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_watch_history
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- silver_watch_history: Watch list history
CREATE TABLE IF NOT EXISTS silver_watch_history (
    wh_key STRING NOT NULL,  -- Composite: w_c_id + w_s_symb
    w_c_id BIGINT NOT NULL,  -- Customer ID
    w_s_symb STRING NOT NULL,  -- Security symbol
    w_dts TIMESTAMP NOT NULL,
    w_action STRING,
    
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
