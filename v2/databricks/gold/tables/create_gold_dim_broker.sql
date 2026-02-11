-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_broker
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- gold_dim_broker: Broker dimension (from HR.csv)
CREATE TABLE IF NOT EXISTS gold_dim_broker (
    sk_broker_id BIGINT NOT NULL,
    broker_id BIGINT NOT NULL,  -- Natural key (EmployeeID where job code = broker)
    broker_name STRING,
    branch STRING,
    office STRING,
    phone STRING,
    is_current BOOLEAN NOT NULL,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
