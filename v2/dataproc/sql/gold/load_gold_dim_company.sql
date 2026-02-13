DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_company;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_company USING DELTA AS
SELECT 
    sc.sk_company_id,
    sc.company_id,
    sc.company_name,
    sc.industry_id,
    COALESCE(si.in_sc_id, 'Unknown') AS sector,
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
    true AS is_current,
    sc.load_timestamp AS start_date,
    CAST('9999-12-31' AS DATE) AS end_date,
    sc.batch_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_companies sc
LEFT JOIN __CATALOG__.__SCHEMA__.silver_industry si ON sc.industry_id = si.in_id
WHERE sc.batch_id = __BATCH_ID__
