-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_time
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- gold_dim_time: Time dimension (hour-level)
CREATE TABLE IF NOT EXISTS gold_dim_time (
    sk_time_id INT NOT NULL,
    time_id INT NOT NULL,  -- Same as sk_time_id
    time_value TIME NOT NULL,
    hour_id INT,
    hour_desc STRING,
    minute_id INT,
    minute_desc STRING,
    second_id INT,
    second_desc STRING,
    market_hours_flag BOOLEAN,
    office_hours_flag BOOLEAN,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
