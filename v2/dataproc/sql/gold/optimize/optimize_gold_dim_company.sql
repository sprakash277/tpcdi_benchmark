-- TPC-DI v2: Optimize gold_dim_company for join performance (run before gold incremental)
-- Placeholders: __CATALOG__, __SCHEMA__

OPTIMIZE __CATALOG__.__SCHEMA__.gold_dim_company ZORDER BY (company_id)
