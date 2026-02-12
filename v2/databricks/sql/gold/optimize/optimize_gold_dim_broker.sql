-- TPC-DI v2: Optimize gold_dim_broker (DimAccount joins here for broker details; Z-Order for pinpoint lookup)
-- Placeholders: __CATALOG__, __SCHEMA__

OPTIMIZE __CATALOG__.__SCHEMA__.gold_dim_broker ZORDER BY (broker_id)
