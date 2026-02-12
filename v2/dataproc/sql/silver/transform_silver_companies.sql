CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_companies AS
SELECT 
    monotonically_increasing_id() AS sk_company_id,
    TRIM(substring(raw_line, 79, 10)) AS company_id,
    TRIM(substring(raw_line, 19, 60)) AS company_name,
    TRIM(substring(raw_line, 93, 2)) AS industry_id,
    TRIM(substring(raw_line, 95, 4)) AS sp_rating,
    TRIM(substring(raw_line, 89, 4)) AS status,
    try_to_date(substring(raw_line, 99, 8), 'yyyyMMdd') AS founding_date,
    TRIM(substring(raw_line, 348, 46)) AS ceo_name,
    TRIM(substring(raw_line, 107, 80)) AS address_line1,
    TRIM(substring(raw_line, 187, 80)) AS address_line2,
    TRIM(substring(raw_line, 267, 12)) AS postal_code,
    TRIM(substring(raw_line, 279, 25)) AS city,
    TRIM(substring(raw_line, 304, 20)) AS state_province,
    TRIM(substring(raw_line, 324, 24)) AS country,
    TRIM(substring(raw_line, 394, 150)) AS description,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_finwire
WHERE _batch_id = __BATCH_ID__
  AND substring(raw_line, 16, 3) = 'CMP'
  AND length(raw_line) >= 394
