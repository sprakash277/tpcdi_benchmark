CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.gold_dim_account AS
SELECT 
    monotonically_increasing_id() AS sk_account_id,
    account_id,
    broker_id,
    customer_id,
    account_name,
    tax_status,
    status_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_accounts
WHERE is_current = true
  AND batch_id = __BATCH_ID__
  AND account_id != -1
