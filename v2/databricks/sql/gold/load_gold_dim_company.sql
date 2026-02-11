CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.gold_dim_company AS
SELECT 
    sc.sk_company_id,
    sc.company_id,
    sc.company_name,
    sc.industry_id,
    si.in_sc_id AS sector,
    sc.status,
    sc.address_line1,
    sc.address_line2,
    sc.postal_code,
    sc.city,
    sc.state_province AS state_prov,
    sc.country,
    sc.description,
    sc.founding_date,
    sc.ceo_name,
    TRUE AS is_current,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_companies sc
LEFT JOIN __CATALOG__.__SCHEMA__.silver_industry si ON sc.industry_id = si.in_id
WHERE sc.batch_id = __BATCH_ID__
