-- TPC-DI v2: Optimize gold_dim_customer for join performance (run before gold incremental)
-- Placeholders: __CATALOG__, __SCHEMA__

OPTIMIZE __CATALOG__.__SCHEMA__.gold_dim_customer ZORDER BY (customer_id)
