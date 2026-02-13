-- TPC-DI v2: DQ rules for silver_companies (aligned with v2 schema: company_id, company_name, founding_date; no cik)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_companies', 'company_name NULL or empty', 'Validation', 'Silver_Companies_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_companies WHERE batch_id = __BATCH_ID__ AND (company_name IS NULL OR TRIM(CAST(company_name AS STRING)) = '') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_companies', 'company_id NULL or empty', 'Validation', 'Silver_Companies_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_companies WHERE batch_id = __BATCH_ID__ AND (company_id IS NULL OR TRIM(CAST(company_id AS STRING)) = '') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_companies', 'duplicate company_id', 'Validation', 'Silver_Companies_Validation', 'Alert'
FROM (SELECT 1 FROM (SELECT company_id FROM __CATALOG__.__SCHEMA__.silver_companies GROUP BY company_id HAVING COUNT(*) > 1) u LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_companies', 'founding_date outside 1900-2100', 'Validation', 'Silver_Companies_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_companies WHERE batch_id = __BATCH_ID__ AND founding_date IS NOT NULL AND (founding_date < CAST('1900-01-01' AS DATE) OR founding_date > CAST('2100-12-31' AS DATE)) LIMIT 1) t;
