-- TPC-DI v2: DQ rules for silver_date (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__ (no batch_id)

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_date', CONCAT('sk_date_id not valid YYYYMMDD format: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Date_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_date WHERE sk_date_id IS NULL OR LENGTH(CAST(sk_date_id AS STRING)) != 8 OR CAST(sk_date_id AS STRING) NOT RLIKE '^[0-9]{8}$') t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_date', 'sk_date_id outside 19000101-21001231', 'Validation', 'Silver_Date_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_date WHERE sk_date_id IS NOT NULL AND LENGTH(CAST(sk_date_id AS STRING)) = 8 AND CAST(sk_date_id AS STRING) RLIKE '^[0-9]{8}$' AND (CAST(sk_date_id AS BIGINT) < 19000101 OR CAST(sk_date_id AS BIGINT) > 21001231) LIMIT 1) t;
