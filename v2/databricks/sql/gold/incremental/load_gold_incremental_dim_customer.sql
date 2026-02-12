-- TPC-DI v2: Gold incremental - gold_dim_customer (Batch 2+)
-- SCD Type 2: Close old versions then insert new versions (no overwrite of sk_customer_id).
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

-- Step 1: Expire old records in Gold (close current version when we have new data for this customer)
MERGE INTO __CATALOG__.__SCHEMA__.gold_dim_customer AS target
USING (
    SELECT customer_id, MIN(COALESCE(effective_date, load_timestamp)) AS effective_date
    FROM __CATALOG__.__SCHEMA__.silver_customers
    WHERE batch_id = __BATCH_ID__
    GROUP BY customer_id
) AS source
ON target.customer_id = source.customer_id
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = source.effective_date,
    target.etl_timestamp = current_timestamp();

-- Step 2: Insert the new version (each row gets its own sk_customer_id from Silver)
INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_customer
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
WHERE batch_id = __BATCH_ID__
  AND is_current = true
  AND customer_id != -1;
