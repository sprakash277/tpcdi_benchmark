-- TPC-DI v2 Dataproc: Create gold_dim_messages (DQ scripts INSERT into this). DROP + CREATE ensures Delta table exists.
-- Placeholders: __CATALOG__, __SCHEMA__

DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_messages;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_messages (
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
