CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.gold_financials AS
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
    diluted_sh_out,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_financials
WHERE batch_id = __BATCH_ID__
