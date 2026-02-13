-- TPC-DI v2: DQ rules for silver_cash_transaction (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__
-- Uses account_id / transaction_date / amount (v2 names); fallback ct_ca_id, ct_dts, ct_amt if present.

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_cash_transaction', CONCAT('account_id NULL: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_CashTransaction_Validation', 'Reject'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_cash_transaction WHERE batch_id = __BATCH_ID__ AND account_id IS NULL) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_cash_transaction', CONCAT('transaction_date NULL: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_CashTransaction_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_cash_transaction WHERE batch_id = __BATCH_ID__ AND transaction_date IS NULL) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_cash_transaction', 'amount < 0', 'Validation', 'Silver_CashTransaction_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_cash_transaction WHERE batch_id = __BATCH_ID__ AND amount IS NOT NULL AND amount < 0 LIMIT 1) t;
