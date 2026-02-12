-- TPC-DI v2: Gold incremental - gold_dim_account (Batch 2+)
-- SCD Type 2: Close old versions then insert new versions with sk_customer_id from gold_dim_customer.
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__
-- Requires: gold_dim_account has is_current, start_date, end_date, sk_customer_id; gold_dim_customer has start_date, end_date.

-- Step 1: Expire old versions in Gold (close records that were updated or deleted in this batch)
MERGE INTO __CATALOG__.__SCHEMA__.gold_dim_account AS target
USING (
    SELECT account_id, effective_date, record_type
    FROM __CATALOG__.__SCHEMA__.silver_accounts
    WHERE batch_id = __BATCH_ID__
      AND record_type IN ('U', 'D')
) AS source
ON target.account_id = source.account_id
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = source.effective_date,
    target.etl_timestamp = current_timestamp();

-- Step 2: Insert new versions with surrogate key lookup (join to gold_dim_customer for sk_customer_id)
INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_account
SELECT 
    monotonically_increasing_id() AS sk_account_id,
    sa.account_id,
    sa.broker_id,
    dc.sk_customer_id,
    sa.customer_id,
    sa.account_name,
    sa.tax_status,
    sa.status_id,
    true AS is_current,
    sa.effective_date AS start_date,
    CAST('9999-12-31' AS DATE) AS end_date,
    sa.batch_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_accounts sa
JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc
    ON sa.customer_id = dc.customer_id
   AND sa.effective_date >= dc.start_date
   AND (dc.end_date IS NULL OR sa.effective_date < dc.end_date)
WHERE sa.batch_id = __BATCH_ID__
  AND sa.record_type IN ('I', 'U')
  AND sa.account_id != -1;
