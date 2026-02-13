-- TPC-DI v2: DQ rules for silver_prospect (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_prospect', 'agency_id NULL or empty', 'Validation', 'Silver_Prospect_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_prospect WHERE batch_id = __BATCH_ID__ AND (agency_id IS NULL OR TRIM(CAST(agency_id AS STRING)) = '') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_prospect', 'last_name and first_name both empty', 'Validation', 'Silver_Prospect_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_prospect WHERE batch_id = __BATCH_ID__ AND TRIM(CAST(COALESCE(last_name, '') AS STRING)) = '' AND TRIM(CAST(COALESCE(first_name, '') AS STRING)) = '' LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_prospect', 'gender not in (M,F,U)', 'Validation', 'Silver_Prospect_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_prospect WHERE batch_id = __BATCH_ID__ AND gender IS NOT NULL AND TRIM(CAST(gender AS STRING)) != '' AND TRIM(CAST(gender AS STRING)) NOT IN ('M', 'F', 'U') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_prospect', 'income < 0', 'Validation', 'Silver_Prospect_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_prospect WHERE batch_id = __BATCH_ID__ AND income IS NOT NULL AND income < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_prospect', 'age outside 0-120', 'Validation', 'Silver_Prospect_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_prospect WHERE batch_id = __BATCH_ID__ AND age IS NOT NULL AND (age < 0 OR age > 120) LIMIT 1) t;
