-- TPC-DI v2: Optimize gold_financials for security valuation and company health (incremental upserts)
-- Placeholders: __CATALOG__, __SCHEMA__

OPTIMIZE __CATALOG__.__SCHEMA__.gold_financials ZORDER BY (co_name_or_cik)
