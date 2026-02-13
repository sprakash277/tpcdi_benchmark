-- TPC-DI v2: DQ rules for silver_companies (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__ (no batch_id)

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_companies', 'company_name NULL or empty', 'Validation', 'Silver_Companies_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_companies WHERE company_name IS NULL OR TRIM(CAST(company_name AS STRING)) = '' LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_companies', 'cik NULL or empty', 'Validation', 'Silver_Companies_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_companies WHERE cik IS NULL OR TRIM(CAST(cik AS STRING)) = '' LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_companies', 'duplicate cik', 'Validation', 'Silver_Companies_Validation', 'Alert'
FROM (SELECT 1 FROM (SELECT cik FROM __CATALOG__.__SCHEMA__.silver_companies GROUP BY cik HAVING COUNT(*) > 1) u LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_companies', 'founding_date not YYYYMMDD format', 'Validation', 'Silver_Companies_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_companies WHERE founding_date IS NOT NULL AND TRIM(CAST(founding_date AS STRING)) != '' AND TRIM(CAST(founding_date AS STRING)) NOT RLIKE '^[0-9]{8}$' LIMIT 1) t;
