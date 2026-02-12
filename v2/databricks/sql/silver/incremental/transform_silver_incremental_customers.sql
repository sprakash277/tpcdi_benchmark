-- TPC-DI v2: Silver incremental - silver_customers (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

WITH incoming_customers AS (
    SELECT 
        monotonically_increasing_id() AS sk_customer_id,
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS customer_id,
        split_part(raw_line, '|', 4) AS tax_id,
        split_part(raw_line, '|', 5) AS status,
        split_part(raw_line, '|', 6) AS last_name,
        split_part(raw_line, '|', 7) AS first_name,
        split_part(raw_line, '|', 8) AS middle_name,
        split_part(raw_line, '|', 9) AS gender,
        try_cast(split_part(raw_line, '|', 10) AS INT) AS tier,
        try_cast(split_part(raw_line, '|', 11) AS DATE) AS dob,
        split_part(raw_line, '|', 12) AS address_line1,
        split_part(raw_line, '|', 13) AS address_line2,
        split_part(raw_line, '|', 14) AS postal_code,
        split_part(raw_line, '|', 15) AS city,
        split_part(raw_line, '|', 16) AS state_prov,
        split_part(raw_line, '|', 17) AS country,
        split_part(raw_line, '|', 18) AS email1,
        split_part(raw_line, '|', 19) AS email2,
        split_part(raw_line, '|', 20) AS local_tax_id,
        split_part(raw_line, '|', 21) AS national_tax_id,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM __CATALOG__.__SCHEMA__.bronze_customer
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) >= 21
),
updates_to_close AS (
    SELECT 
        customer_id,
        CAST(MIN(cdc_dsn) AS TIMESTAMP) AS new_effective_date
    FROM incoming_customers
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY customer_id
)
MERGE INTO __CATALOG__.__SCHEMA__.silver_customers AS target
USING updates_to_close AS src
ON target.customer_id = src.customer_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = CAST(src.new_effective_date AS TIMESTAMP);

WITH incoming_customers AS (
    SELECT 
        monotonically_increasing_id() AS sk_customer_id,
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS customer_id,
        split_part(raw_line, '|', 4) AS tax_id,
        split_part(raw_line, '|', 5) AS status,
        split_part(raw_line, '|', 6) AS last_name,
        split_part(raw_line, '|', 7) AS first_name,
        split_part(raw_line, '|', 8) AS middle_name,
        split_part(raw_line, '|', 9) AS gender,
        try_cast(split_part(raw_line, '|', 10) AS INT) AS tier,
        try_cast(split_part(raw_line, '|', 11) AS DATE) AS dob,
        split_part(raw_line, '|', 12) AS address_line1,
        split_part(raw_line, '|', 13) AS address_line2,
        split_part(raw_line, '|', 14) AS postal_code,
        split_part(raw_line, '|', 15) AS city,
        split_part(raw_line, '|', 16) AS state_prov,
        split_part(raw_line, '|', 17) AS country,
        split_part(raw_line, '|', 18) AS email1,
        split_part(raw_line, '|', 19) AS email2,
        split_part(raw_line, '|', 20) AS local_tax_id,
        split_part(raw_line, '|', 21) AS national_tax_id,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM __CATALOG__.__SCHEMA__.bronze_customer
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) >= 21
)
INSERT INTO __CATALOG__.__SCHEMA__.silver_customers
SELECT 
    sk_customer_id,
    customer_id,
    tax_id,
    status,
    last_name,
    first_name,
    middle_name,
    gender,
    tier,
    dob,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_prov,
    country,
    email1,
    email2,
    local_tax_id,
    national_tax_id,
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    NULL AS end_date,
    batch_id,
    load_timestamp,
    cdc_flag AS record_type
FROM incoming_customers
WHERE cdc_flag IN ('I', 'U');
