-- TPC-DI v2: Gold incremental - gold_dim_customer (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

MERGE INTO __CATALOG__.__SCHEMA__.gold_dim_customer AS target
USING (
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
        national_tax_id
    FROM __CATALOG__.__SCHEMA__.silver_customers
    WHERE is_current = true
      AND batch_id = __BATCH_ID__
      AND customer_id != -1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_date DESC) = 1
) AS source
ON target.customer_id = source.customer_id
WHEN MATCHED THEN UPDATE SET
    target.sk_customer_id = source.sk_customer_id,
    target.tax_id = source.tax_id,
    target.status = source.status,
    target.last_name = source.last_name,
    target.first_name = source.first_name,
    target.middle_name = source.middle_name,
    target.gender = source.gender,
    target.tier = source.tier,
    target.dob = source.dob,
    target.address_line1 = source.address_line1,
    target.address_line2 = source.address_line2,
    target.postal_code = source.postal_code,
    target.city = source.city,
    target.state_prov = source.state_prov,
    target.country = source.country,
    target.email1 = source.email1,
    target.email2 = source.email2,
    target.local_tax_id = source.local_tax_id,
    target.national_tax_id = source.national_tax_id,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
