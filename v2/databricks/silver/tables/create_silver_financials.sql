-- ============================================================================
-- TPC-DI v2: Silver Layer - Create silver_financials
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- silver_financials: Financial records from FINWIRE (FIN)
CREATE TABLE IF NOT EXISTS silver_financials (
    co_name_or_cik STRING NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    qtr_start_date DATE,
    posting_date DATE,
    revenue DOUBLE,
    earnings DOUBLE,
    eps DOUBLE,
    diluted_eps DOUBLE,
    margin DOUBLE,
    inventory DOUBLE,
    assets DOUBLE,
    liabilities DOUBLE,
    sh_out BIGINT,
    diluted_sh_out BIGINT,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (co_name_or_cik, year, quarter)  -- Composite key for MERGE
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
