-- TPC-DI v2: Create gold_dim_messages if not exists (incremental-only table, no batch load)
-- Placeholders: __CATALOG__, __SCHEMA__

CREATE TABLE IF NOT EXISTS __CATALOG__.__SCHEMA__.gold_dim_messages (
    message_timestamp TIMESTAMP NOT NULL,
    batch_id INT NOT NULL,
    originating_table STRING NOT NULL,
    message_text STRING NOT NULL,
    message_type STRING NOT NULL,
    component_name STRING,
    severity STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
