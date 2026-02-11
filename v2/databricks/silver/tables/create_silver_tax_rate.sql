-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_tax_rate
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- silver_tax_rate: Tax rate reference
CREATE TABLE IF NOT EXISTS silver_tax_rate (
    tx_id STRING NOT NULL,
    tx_name STRING,
    tx_rate DOUBLE,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
