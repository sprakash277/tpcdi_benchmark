-- TPC-DI v2: DQ rules for silver_securities (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__ (no batch_id)

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_securities', CONCAT('symbol NULL or empty: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Security_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_securities WHERE symbol IS NULL OR TRIM(CAST(symbol AS STRING)) = '') t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_securities', CONCAT('duplicate symbol: ', CAST(cnt AS STRING), ' key(s)'), 'Validation', 'Silver_Security_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM (SELECT symbol FROM __CATALOG__.__SCHEMA__.silver_securities GROUP BY symbol HAVING COUNT(*) > 1) u) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_securities', 'name NULL or empty', 'Validation', 'Silver_Security_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_securities WHERE name IS NULL OR TRIM(CAST(name AS STRING)) = '' LIMIT 1) t;
