-- TPC-DI v2: Optimize gold_prospect for join performance (run before gold incremental)
-- Placeholders: __CATALOG__, __SCHEMA__

OPTIMIZE __CATALOG__.__SCHEMA__.gold_prospect ZORDER BY (agency_id)
