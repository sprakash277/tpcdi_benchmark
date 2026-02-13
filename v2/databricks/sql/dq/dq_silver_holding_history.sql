-- TPC-DI v2: DQ rules for silver_holding_history (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'hh_h_t_id NULL', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND hh_h_t_id IS NULL LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'hh_t_id NULL', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND hh_t_id IS NULL LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'hh_before_qty < 0', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND hh_before_qty IS NOT NULL AND hh_before_qty < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'hh_after_qty < 0', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND hh_after_qty IS NOT NULL AND hh_after_qty < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'record_type not in (I,U,D)', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND record_type IS NOT NULL AND TRIM(CAST(record_type AS STRING)) != '' AND TRIM(CAST(record_type AS STRING)) NOT IN ('I', 'U', 'D') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'quantity != hh_after_qty', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND quantity IS NOT NULL AND hh_after_qty IS NOT NULL AND quantity != hh_after_qty LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'purchase_price < 0', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND purchase_price IS NOT NULL AND purchase_price < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', CONCAT('account_id NULL (trade not in silver_trades): ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND hh_t_id IS NOT NULL AND account_id IS NULL) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'symbol NULL or empty for linked trade', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND hh_t_id IS NOT NULL AND (symbol IS NULL OR TRIM(CAST(symbol AS STRING)) = '') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'duplicate hh_h_t_id in batch 1', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM (SELECT hh_h_t_id FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = 1 GROUP BY hh_h_t_id HAVING COUNT(*) > 1) u LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'holding_date/effective_date in future', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND (effective_date IS NOT NULL AND effective_date > current_timestamp() OR holding_date IS NOT NULL AND holding_date > current_timestamp()) LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'hh_after_qty exceeds 1e12', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND hh_after_qty IS NOT NULL AND hh_after_qty > 1e12 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', CONCAT('hh_t_id not in silver_trades: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_holding_history h WHERE h.batch_id = __BATCH_ID__ AND h.hh_t_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades t WHERE t.trade_id = h.hh_t_id AND t.batch_id = __BATCH_ID__)) u WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', CONCAT('account_id not in silver_accounts: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_holding_history h WHERE h.batch_id = __BATCH_ID__ AND h.account_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_accounts a WHERE a.account_id = h.account_id)) u WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', CONCAT('symbol not in silver_securities: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_holding_history h WHERE h.batch_id = __BATCH_ID__ AND TRIM(CAST(h.symbol AS STRING)) != '' AND NOT EXISTS (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_securities s WHERE TRIM(CAST(s.symbol AS STRING)) = TRIM(CAST(h.symbol AS STRING)))) u WHERE cnt > 0;
