-- TPC-DI v2: DQ rules for silver_customers (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__
-- Ensures gold_dim_messages exists before running (run create_gold_dim_messages.sql first).

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', CONCAT('customer_id/tax_id NULL: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Customer_Validation', 'Reject'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND (customer_id IS NULL OR tax_id IS NULL)) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', CONCAT('tier not in (1,2,3): ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND tier IS NOT NULL AND COALESCE(tier, 0) NOT IN (1, 2, 3)) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', CONCAT('dob in future: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND dob IS NOT NULL AND dob > current_date()) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', CONCAT('duplicate customer_id within batch: ', CAST(cnt AS STRING), ' key(s)'), 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM (SELECT customer_id FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ GROUP BY customer_id HAVING COUNT(*) > 1) u) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', CONCAT('end_date < effective_date: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND end_date IS NOT NULL AND effective_date IS NOT NULL AND end_date < effective_date) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', CONCAT('gender not in (M,F,U): ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND gender IS NOT NULL AND TRIM(CAST(gender AS STRING)) != '' AND TRIM(CAST(gender AS STRING)) NOT IN ('M', 'F', 'U')) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', CONCAT('status NULL or empty: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND (status IS NULL OR TRIM(CAST(status AS STRING)) = '')) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'status not in (ACTV,INAC,NEW,UPDCUST,INACT)', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND status IS NOT NULL AND TRIM(CAST(status AS STRING)) != '' AND TRIM(CAST(status AS STRING)) NOT IN ('ACTV', 'INAC', 'ACTIVE', 'NEW', 'UPDCUST', 'INACT') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'first_name NULL or empty', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND (first_name IS NULL OR TRIM(CAST(first_name AS STRING)) = '') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'last_name NULL or empty', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND (last_name IS NULL OR TRIM(CAST(last_name AS STRING)) = '') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'tax_id empty for non-null customer', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND customer_id IS NOT NULL AND TRIM(CAST(tax_id AS STRING)) = '' LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'duplicate tax_id within batch', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM (SELECT TRIM(CAST(tax_id AS STRING)) AS tid FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ GROUP BY TRIM(CAST(tax_id AS STRING)) HAVING COUNT(*) > 1) u LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'dob before 1900-01-01', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND dob IS NOT NULL AND dob < CAST('1900-01-01' AS DATE) LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'effective_date in future', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND effective_date IS NOT NULL AND effective_date > current_timestamp() LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'postal_code length > 20', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND TRIM(CAST(postal_code AS STRING)) != '' AND LENGTH(TRIM(CAST(postal_code AS STRING))) > 20 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'email1 missing @ when non-empty', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND TRIM(CAST(COALESCE(email1, '') AS STRING)) != '' AND INSTR(TRIM(CAST(email1 AS STRING)), '@') <= 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_customers', 'email2 missing @ when non-empty', 'Validation', 'Silver_Customer_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_customers WHERE batch_id = __BATCH_ID__ AND TRIM(CAST(COALESCE(email2, '') AS STRING)) != '' AND INSTR(TRIM(CAST(email2 AS STRING)), '@') <= 0 LIMIT 1) t;
