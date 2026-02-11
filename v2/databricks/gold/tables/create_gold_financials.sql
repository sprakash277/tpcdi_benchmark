-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_financials
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_financials: Financial reporting (SCD Type 1 - latest only)
CREATE TABLE IF NOT EXISTS gold_financials (
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
    etl_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (co_name_or_cik, year, quarter)
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
