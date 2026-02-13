DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_financials;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_financials USING DELTA AS
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
    sf.batch_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_financials sf
LEFT JOIN __CATALOG__.__SCHEMA__.gold_dim_company dc
    ON sf.co_name_or_cik = dc.company_id
   AND dc.is_current = true
   AND sf.posting_date >= dc.start_date
   AND (dc.end_date IS NULL OR sf.posting_date < dc.end_date)
WHERE sf.batch_id = __BATCH_ID__
