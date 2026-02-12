-- TPC-DI v2: Gold incremental - gold_dim_security (Batch 2+)
-- SCD Type 2: Close old versions then insert new versions with sk_company_id from gold_dim_company.
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__
-- Requires: gold_dim_security has is_current, start_date, end_date, sk_company_id. gold_dim_company has start_date, end_date.

-- Deduplicate source so only the LATEST record per symbol tries to CLOSE the existing Gold record
WITH latest_silver_securities AS (
    SELECT symbol, COALESCE(effective_date, load_timestamp) AS effective_date
    FROM __CATALOG__.__SCHEMA__.silver_securities
    WHERE batch_id = __BATCH_ID__
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY COALESCE(effective_date, load_timestamp) DESC) = 1
)
MERGE INTO __CATALOG__.__SCHEMA__.gold_dim_security AS target
USING latest_silver_securities AS source
ON target.symbol = source.symbol
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = source.effective_date,
    target.etl_timestamp = current_timestamp();

-- Step 2: Insert new versions with Company SK lookup (LEFT JOIN for late-arriving companies → -1)
INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_security
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
    COALESCE(ss.effective_date, ss.load_timestamp) AS start_date,
    CAST('9999-12-31' AS DATE) AS end_date,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_securities ss
LEFT JOIN __CATALOG__.__SCHEMA__.gold_dim_company dc
    ON ss.co_name_or_cik = dc.company_id
   AND COALESCE(ss.effective_date, ss.load_timestamp) >= dc.start_date
   AND (dc.end_date IS NULL OR COALESCE(ss.effective_date, ss.load_timestamp) < dc.end_date)
WHERE ss.batch_id = __BATCH_ID__;
