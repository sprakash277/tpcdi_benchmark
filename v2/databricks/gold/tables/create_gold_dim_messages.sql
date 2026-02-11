-- ============================================================================
-- TPC-DI v2: Gold Layer - Create gold_dim_messages
-- ============================================================================
-- Set catalog and schema
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};



-- gold_dim_messages: Audit/logging table for data quality and referential integrity
CREATE TABLE IF NOT EXISTS gold_dim_messages (
    message_timestamp TIMESTAMP NOT NULL,
    batch_id INT NOT NULL,
    originating_table STRING NOT NULL,  -- Source table (e.g., 'FactTrade', 'DimCustomer')
    message_text STRING NOT NULL,
    message_type STRING NOT NULL,  -- 'Alert', 'Reject', 'Info'
    component_name STRING,  -- Component that generated the message (e.g., 'Silver_Customer_Validation')
    severity STRING  -- 'Alert', 'Reject', 'Warning', 'Info'
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
