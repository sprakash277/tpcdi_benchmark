-- TPC-DI v2: Gold incremental - gold_financials (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

MERGE INTO __CATALOG__.__SCHEMA__.gold_financials AS target
USING (
    SELECT 
        co_name_or_cik,
        year,
        quarter,
        qtr_start_date,
        posting_date,
        revenue,
        earnings,
        eps,
        diluted_eps,
        margin,
        inventory,
        assets,
        liabilities,
        sh_out,
        diluted_sh_out
    FROM __CATALOG__.__SCHEMA__.silver_financials
    WHERE batch_id = __BATCH_ID__
) AS source
ON target.co_name_or_cik = source.co_name_or_cik
   AND target.year = source.year
   AND target.quarter = source.quarter
WHEN MATCHED THEN UPDATE SET
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
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
