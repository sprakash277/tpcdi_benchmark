CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.gold_dim_security AS
SELECT 
    monotonically_increasing_id() AS sk_security_id,
    ss.symbol AS security_id,
    ss.symbol,
    ss.issue_type,
    ss.status,
    ss.name,
    ss.ex_id AS exchange_id,
    ss.sh_out AS shares_outstanding,
    ss.first_trade_date,
    ss.first_trade_exchg AS first_trade_exchange,
    ss.dividend,
    COALESCE(dc.sk_company_id, -1) AS sk_company_id,
    ss.co_name_or_cik AS company_id,
    true AS is_current,
    ss.load_timestamp AS start_date,
    CAST('9999-12-31' AS DATE) AS end_date,
    ss.batch_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_securities ss
LEFT JOIN __CATALOG__.__SCHEMA__.gold_dim_company dc
    ON ss.co_name_or_cik = dc.company_id
   AND dc.is_current = true
   AND ss.load_timestamp >= dc.start_date
   AND (dc.end_date IS NULL OR ss.load_timestamp < dc.end_date)
WHERE ss.batch_id = __BATCH_ID__
