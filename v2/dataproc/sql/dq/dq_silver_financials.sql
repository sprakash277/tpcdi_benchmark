-- TPC-DI v2: DQ rules for silver_financials (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__ (no batch_id)

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_financials', 'year outside 1900-2100', 'Validation', 'Silver_Financials_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_financials WHERE year IS NOT NULL AND (year < 1900 OR year > 2100) LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_financials', 'quarter not in (1,2,3,4)', 'Validation', 'Silver_Financials_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_financials WHERE quarter IS NOT NULL AND COALESCE(quarter, 0) NOT IN (1, 2, 3, 4) LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_financials', 'revenue < 0', 'Validation', 'Silver_Financials_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_financials WHERE revenue IS NOT NULL AND revenue < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_financials', 'earnings < 0', 'Validation', 'Silver_Financials_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_financials WHERE earnings IS NOT NULL AND earnings < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_financials', 'assets < 0', 'Validation', 'Silver_Financials_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_financials WHERE assets IS NOT NULL AND assets < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_financials', 'liabilities < 0', 'Validation', 'Silver_Financials_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_financials WHERE liabilities IS NOT NULL AND liabilities < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), 1, 'silver_financials', 'co_name_or_cik NULL or empty', 'Validation', 'Silver_Financials_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_financials WHERE co_name_or_cik IS NULL OR TRIM(CAST(co_name_or_cik AS STRING)) = '' LIMIT 1) t;
