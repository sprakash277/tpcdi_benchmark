-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_customers
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_customers: Customer dimension with SCD Type 2
CREATE TABLE IF NOT EXISTS silver_customers (
    -- Surrogate Key
    sk_customer_id BIGINT,
    
    -- Business Key
    customer_id BIGINT NOT NULL,
    
    -- Attributes
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
    
    -- SCD Type 2 Columns
    is_current BOOLEAN NOT NULL,
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    
    -- Metadata
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    record_type STRING  -- I=Insert, U=Update, D=Delete
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
