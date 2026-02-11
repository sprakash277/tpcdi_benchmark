CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.gold_dim_customer AS
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
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_customers
WHERE is_current = true
  AND batch_id = __BATCH_ID__
  AND customer_id != -1
