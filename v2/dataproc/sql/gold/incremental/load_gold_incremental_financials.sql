-- TPC-DI v2: Gold incremental - gold_financials (Batch 2+)
-- Join to gold_dim_company for sk_company_id. Deduplicate source so one row per (co_name_or_cik, year, quarter).
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

-- Deduplicate source so only the LATEST record per (co_name_or_cik, year, quarter) updates the target
WITH latest_silver_financials AS (
    SELECT 
        COALESCE(dc.sk_company_id, -1) AS sk_company_id,
        sf.co_name_or_cik,
        sf.year,
        sf.quarter,
        sf.qtr_start_date,
        sf.posting_date,
        sf.revenue,
        sf.earnings,
        sf.eps,
        sf.diluted_eps,
        sf.margin,
        sf.inventory,
        sf.assets,
        sf.liabilities,
        sf.sh_out,
        sf.diluted_sh_out,
        sf.batch_id
    FROM __CATALOG__.__SCHEMA__.silver_financials sf
    LEFT JOIN __CATALOG__.__SCHEMA__.gold_dim_company dc
        ON sf.co_name_or_cik = dc.company_id
        AND sf.posting_date >= dc.start_date
        AND (dc.end_date IS NULL OR sf.posting_date < dc.end_date)
    WHERE sf.batch_id = __BATCH_ID__
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sf.co_name_or_cik, sf.year, sf.quarter ORDER BY sf.posting_date DESC) = 1
)
MERGE INTO __CATALOG__.__SCHEMA__.gold_financials AS target
USING latest_silver_financials AS source
ON target.co_name_or_cik = source.co_name_or_cik
   AND target.year = source.year
   AND target.quarter = source.quarter
WHEN MATCHED THEN UPDATE SET
    target.sk_company_id = source.sk_company_id,
    target.qtr_start_date = source.qtr_start_date,
    target.posting_date = source.posting_date,
    target.revenue = source.revenue,
    target.earnings = source.earnings,
    target.eps = source.eps,
    target.diluted_eps = source.diluted_eps,
    target.margin = source.margin,
    target.inventory = source.inventory,
    target.assets = source.assets,
    target.liabilities = source.liabilities,
    target.sh_out = source.sh_out,
    target.diluted_sh_out = source.diluted_sh_out,
    target.batch_id = source.batch_id,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    sk_company_id, co_name_or_cik, year, quarter, qtr_start_date, posting_date,
    revenue, earnings, eps, diluted_eps, margin, inventory, assets,
    liabilities, sh_out, diluted_sh_out, batch_id, etl_timestamp
) VALUES (
    source.sk_company_id, source.co_name_or_cik, source.year, source.quarter,
    source.qtr_start_date, source.posting_date, source.revenue, source.earnings,
    source.eps, source.diluted_eps, source.margin, source.inventory,
    source.assets, source.liabilities, source.sh_out, source.diluted_sh_out,
    source.batch_id, current_timestamp()
);
