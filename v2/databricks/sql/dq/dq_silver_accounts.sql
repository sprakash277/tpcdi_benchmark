-- TPC-DI v2: DQ rules for silver_accounts (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_accounts', CONCAT('customer_id not in silver_customers: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Account_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_accounts a WHERE a.batch_id = __BATCH_ID__ AND NOT EXISTS (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers c WHERE c.customer_id = a.customer_id)) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_accounts', CONCAT('customer_id NULL in silver_accounts: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Account_Validation', 'Reject'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_accounts WHERE batch_id = __BATCH_ID__ AND customer_id IS NULL) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_accounts', 'account_id NULL in silver_accounts', 'Validation', 'Silver_Account_Validation', 'Reject'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_accounts WHERE batch_id = __BATCH_ID__ AND account_id IS NULL LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_accounts', CONCAT('end_date < effective_date: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Account_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_accounts WHERE batch_id = __BATCH_ID__ AND end_date IS NOT NULL AND effective_date IS NOT NULL AND end_date < effective_date) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_accounts', 'account_name NULL or empty', 'Validation', 'Silver_Account_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_accounts WHERE batch_id = __BATCH_ID__ AND (account_name IS NULL OR TRIM(CAST(account_name AS STRING)) = '') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_accounts', 'duplicate account_id within batch', 'Validation', 'Silver_Account_Validation', 'Alert'
FROM (SELECT 1 FROM (SELECT account_id FROM __CATALOG__.__SCHEMA__.silver_accounts WHERE batch_id = __BATCH_ID__ GROUP BY account_id HAVING COUNT(*) > 1) u LIMIT 1) t;
