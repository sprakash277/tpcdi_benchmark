-- TPC-DI v2: DQ rules for silver_trades (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', CONCAT('bid_price NULL or <= 0: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND (bid_price IS NULL OR bid_price <= 0)) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', CONCAT('quantity NULL or <= 0: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND (quantity IS NULL OR quantity <= 0)) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', CONCAT('duplicate trade_id within batch: ', CAST(cnt AS STRING), ' key(s)'), 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM (SELECT trade_id FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ GROUP BY trade_id HAVING COUNT(*) > 1) u) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', CONCAT('account_id NULL: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Trade_Validation', 'Reject'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND account_id IS NULL) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', CONCAT('trade_price <= 0: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND trade_price IS NOT NULL AND trade_price <= 0) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', 'commission < 0', 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND commission IS NOT NULL AND commission < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', 'tax < 0', 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND tax IS NOT NULL AND tax < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', CONCAT('trade_dts NULL: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND trade_dts IS NULL) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', 'record_type not in (I,U,D)', 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND record_type IS NOT NULL AND TRIM(CAST(record_type AS STRING)) != '' AND TRIM(CAST(record_type AS STRING)) NOT IN ('I', 'U', 'D') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', 'symbol NULL or empty', 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND (symbol IS NULL OR TRIM(CAST(symbol AS STRING)) = '') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', 'trade_dts in future', 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND trade_dts IS NOT NULL AND trade_dts > current_timestamp() LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', 'charge < 0', 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND charge IS NOT NULL AND charge < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', 'quantity exceeds 1e9', 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND quantity IS NOT NULL AND quantity > 1e9 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', 'end_date < effective_date', 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades WHERE batch_id = __BATCH_ID__ AND end_date IS NOT NULL AND effective_date IS NOT NULL AND end_date < effective_date LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', CONCAT('account_id not in silver_accounts: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_trades t WHERE t.batch_id = __BATCH_ID__ AND t.account_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_accounts a WHERE a.account_id = t.account_id)) u WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_trades', CONCAT('symbol not in silver_securities: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Trade_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_trades t WHERE t.batch_id = __BATCH_ID__ AND TRIM(CAST(t.symbol AS STRING)) != '' AND NOT EXISTS (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_securities s WHERE TRIM(CAST(s.symbol AS STRING)) = TRIM(CAST(t.symbol AS STRING)))) u WHERE cnt > 0;
