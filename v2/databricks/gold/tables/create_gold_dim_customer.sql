-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_customer
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- gold_dim_customer: Customer dimension (SCD Type 2 in Silver, current only in Gold)
CREATE TABLE IF NOT EXISTS gold_dim_customer (
    sk_customer_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,  -- Natural key
    tax_id STRING,
    status STRING,
    last_name STRING,
    first_name STRING,
    middle_name STRING,
    gender STRING,
    tier INT,
    dob DATE,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state_prov STRING,
    country STRING,
    email1 STRING,
    email2 STRING,
    local_tax_id STRING,
    national_tax_id STRING,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
