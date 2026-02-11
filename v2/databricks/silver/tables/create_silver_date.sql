-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_date
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_date: Date dimension (parsed from bronze_date)
CREATE TABLE IF NOT EXISTS silver_date (
    sk_date_id INT NOT NULL,
    date_value DATE NOT NULL,
    date_desc STRING,
    calendar_year_id INT,
    calendar_year_desc STRING,
    calendar_qtr_id INT,
    calendar_qtr_desc STRING,
    calendar_month_id INT,
    calendar_month_desc STRING,
    calendar_week_id INT,
    calendar_week_desc STRING,
    day_of_week_num INT,
    day_of_week_desc STRING,
    fiscal_year_id INT,
    fiscal_year_desc STRING,
    fiscal_qtr_id INT,
    fiscal_qtr_desc STRING,
    holiday_flag BOOLEAN,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
