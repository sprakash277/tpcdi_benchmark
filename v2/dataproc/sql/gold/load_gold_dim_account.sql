DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_account;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_account AS
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
    COALESCE(sa.effective_date, sa.load_timestamp) AS start_date,
    CAST('9999-12-31' AS DATE) AS end_date,
    sa.batch_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_accounts sa
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc
    ON sa.customer_id = dc.customer_id
   AND dc.is_current = true
   AND COALESCE(sa.effective_date, sa.load_timestamp) >= dc.start_date
   AND (dc.end_date IS NULL OR COALESCE(sa.effective_date, sa.load_timestamp) < dc.end_date)
WHERE sa.is_current = true
  AND sa.batch_id = __BATCH_ID__
  AND sa.account_id != -1
