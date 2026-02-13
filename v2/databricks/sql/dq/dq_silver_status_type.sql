-- TPC-DI v2: DQ rules for silver_status_type (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__ (no batch_id)

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_status_type', CONCAT('st_id NULL or empty: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_StatusType_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_status_type WHERE st_id IS NULL OR TRIM(CAST(st_id AS STRING)) = '') t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_status_type', CONCAT('st_name NULL or empty: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_StatusType_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_status_type WHERE st_name IS NULL OR TRIM(CAST(st_name AS STRING)) = '') t WHERE cnt > 0;
