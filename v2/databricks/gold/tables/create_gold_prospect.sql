-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_prospect
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- gold_prospect: Prospect table (from Prospect.csv)
CREATE TABLE IF NOT EXISTS gold_prospect (
    agency_id STRING NOT NULL,
    last_name STRING,
    first_name STRING,
    middle_initial STRING,
    gender STRING,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state STRING,
    country STRING,
    phone STRING,
    income INT,
    number_cars INT,
    number_children INT,
    marital_status STRING,
    age INT,
    credit_rating INT,
    own_or_rent_flag STRING,
    employer STRING,
    number_credit_cards INT,
    net_worth INT,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
