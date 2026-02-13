DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_customer;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_customer AS
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
    true AS is_current,
    COALESCE(effective_date, load_timestamp) AS start_date,
    CAST('9999-12-31' AS DATE) AS end_date,
    batch_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_customers
WHERE is_current = true
  AND batch_id = __BATCH_ID__
  AND customer_id != -1
