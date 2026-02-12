-- TPC-DI v2: Gold incremental - gold_dim_account (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

MERGE INTO __CATALOG__.__SCHEMA__.gold_dim_account AS target
USING (
    SELECT 
        monotonically_increasing_id() AS sk_account_id,
        account_id,
        broker_id,
        customer_id,
        account_name,
        tax_status,
        status_id
    FROM __CATALOG__.__SCHEMA__.silver_accounts
    WHERE is_current = true
      AND batch_id = __BATCH_ID__
      AND account_id != -1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY effective_date DESC) = 1
) AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET
    target.broker_id = source.broker_id,
    target.customer_id = source.customer_id,
    target.account_name = source.account_name,
    target.tax_status = source.tax_status,
    target.status_id = source.status_id,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
