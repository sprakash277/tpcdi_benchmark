-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_time
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_time: Time dimension (parsed from bronze_time)
CREATE TABLE IF NOT EXISTS silver_time (
    sk_time_id INT NOT NULL,
    time_value TIME NOT NULL,
    hour_id INT,
    hour_desc STRING,
    minute_id INT,
    minute_desc STRING,
    second_id INT,
    second_desc STRING,
    market_hours_flag BOOLEAN,
    office_hours_flag BOOLEAN,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
