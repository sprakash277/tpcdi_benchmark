-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_fact_cash_balances
-- ============================================================================
-- Set catalog and schema
-- USE CATALOG ${var.catalog};
-- USE SCHEMA ${var.schema};



-- gold_fact_cash_balances: Cash balances fact (aggregated)
CREATE TABLE IF NOT EXISTS gold_fact_cash_balances (
    sk_date_id INT NOT NULL,
    sk_account_id BIGINT NOT NULL,
    sk_customer_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    cash_balance DOUBLE,  -- Sum of CT_AMT by account/date
    transaction_count BIGINT,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
