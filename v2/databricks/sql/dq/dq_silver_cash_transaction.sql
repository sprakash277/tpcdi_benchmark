-- TPC-DI v2: DQ rules for silver_cash_transaction (v2 schema: ct_ca_id, ct_dts, ct_amt)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_cash_transaction', CONCAT('ct_ca_id NULL: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_CashTransaction_Validation', 'Reject'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_cash_transaction WHERE batch_id = __BATCH_ID__ AND ct_ca_id IS NULL) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_cash_transaction', CONCAT('ct_dts NULL: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_CashTransaction_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_cash_transaction WHERE batch_id = __BATCH_ID__ AND ct_dts IS NULL) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_cash_transaction', 'ct_amt < 0', 'Validation', 'Silver_CashTransaction_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_cash_transaction WHERE batch_id = __BATCH_ID__ AND ct_amt IS NOT NULL AND ct_amt < 0 LIMIT 1) t;
