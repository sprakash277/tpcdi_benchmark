-- TPC-DI v2: DQ rules for silver_trade_type (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__ (no batch_id)

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_trade_type', CONCAT('tt_id NULL or empty: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_TradeType_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_trade_type WHERE tt_id IS NULL OR TRIM(CAST(tt_id AS STRING)) = '') t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_trade_type', CONCAT('tt_name NULL or empty: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_TradeType_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_trade_type WHERE tt_name IS NULL OR TRIM(CAST(tt_name AS STRING)) = '') t WHERE cnt > 0;
