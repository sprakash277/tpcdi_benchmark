-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_date
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- gold_dim_date: Date dimension
CREATE TABLE IF NOT EXISTS gold_dim_date (
    sk_date_id INT NOT NULL,
    date_id INT NOT NULL,  -- Same as sk_date_id
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
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
