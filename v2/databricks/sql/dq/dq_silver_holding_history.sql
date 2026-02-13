-- TPC-DI v2: DQ rules for silver_holding_history (v2 batch schema: hh_h_t_id, hh_t_id, hh_before_qty, hh_after_qty, effective_date, record_type; no quantity/symbol/account_id/purchase_price/holding_date)
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
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'duplicate hh_h_t_id in batch 1', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM (SELECT hh_h_t_id FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = 1 GROUP BY hh_h_t_id HAVING COUNT(*) > 1) u LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'effective_date in future', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND effective_date IS NOT NULL AND effective_date > current_timestamp() LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', 'hh_after_qty exceeds 1e12', 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_holding_history WHERE batch_id = __BATCH_ID__ AND hh_after_qty IS NOT NULL AND hh_after_qty > 1e12 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_holding_history', CONCAT('hh_t_id not in silver_trades: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_HoldingHistory_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_holding_history h WHERE h.batch_id = __BATCH_ID__ AND h.hh_t_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_trades t WHERE t.trade_id = h.hh_t_id AND t.batch_id = __BATCH_ID__)) u WHERE cnt > 0;
