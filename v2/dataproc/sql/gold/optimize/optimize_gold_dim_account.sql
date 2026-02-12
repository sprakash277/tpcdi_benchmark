-- TPC-DI v2: Optimize gold_dim_account for join performance (critical: almost every fact table joins here)
-- Placeholders: __CATALOG__, __SCHEMA__

OPTIMIZE __CATALOG__.__SCHEMA__.gold_dim_account ZORDER BY (account_id)
