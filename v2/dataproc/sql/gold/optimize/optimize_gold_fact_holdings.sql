-- TPC-DI v2: Optimize gold_fact_holdings (table grows significantly; updates and consistency checks need Z-Order)
-- Placeholders: __CATALOG__, __SCHEMA__

OPTIMIZE __CATALOG__.__SCHEMA__.gold_fact_holdings ZORDER BY (sk_account_id, sk_security_id)
