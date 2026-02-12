-- TPC-DI v2: Gold incremental - gold_dim_company (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

MERGE INTO __CATALOG__.__SCHEMA__.gold_dim_company AS target
USING (
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
        sc.ceo_name
    FROM __CATALOG__.__SCHEMA__.silver_companies sc
    LEFT JOIN __CATALOG__.__SCHEMA__.silver_industry si ON sc.industry_id = si.in_id
    WHERE sc.batch_id = __BATCH_ID__
) AS source
ON target.company_id = source.company_id
WHEN MATCHED THEN UPDATE SET
    target.company_name = source.company_name,
    target.industry_id = source.industry_id,
    target.sector = source.sector,
    target.status = source.status,
    target.address_line1 = source.address_line1,
    target.address_line2 = source.address_line2,
    target.postal_code = source.postal_code,
    target.city = source.city,
    target.state_prov = source.state_prov,
    target.country = source.country,
    target.description = source.description,
    target.founding_date = source.founding_date,
    target.ceo_name = source.ceo_name,
    target.is_current = true,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    sk_company_id, company_id, company_name, industry_id, sector, status,
    address_line1, address_line2, postal_code, city, state_prov, country,
    description, founding_date, ceo_name, is_current, etl_timestamp
) VALUES (
    source.sk_company_id, source.company_id, source.company_name, source.industry_id,
    source.sector, source.status, source.address_line1, source.address_line2,
    source.postal_code, source.city, source.state_prov, source.country,
    source.description, source.founding_date, source.ceo_name, true, current_timestamp()
);
