-- TPC-DI v2: Optimize gold_fact_trade for join/range performance (run before gold incremental)
-- Placeholders: __CATALOG__, __SCHEMA__

OPTIMIZE __CATALOG__.__SCHEMA__.gold_fact_trade ZORDER BY (sk_date_id, sk_account_id)
