-- ============================================================================
-- TPC-DI v2: Bronze Layer - Create bronze_cash_transaction
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- bronze_cash_transaction: Pipe-delimited cash transactions
CREATE TABLE IF NOT EXISTS bronze_cash_transaction (
    raw_line STRING,                   -- Pipe-delimited (4 cols historical, 6 cols incremental)
    _batch_id INT,
    _load_timestamp TIMESTAMP,
    _source_file STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
