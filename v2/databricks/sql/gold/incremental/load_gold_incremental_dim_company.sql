-- TPC-DI v2: Gold incremental - gold_dim_company (Batch 2+)
-- SCD Type 2: Close old versions then insert new versions (point-in-time reporting).
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__
-- Requires: gold_dim_company has is_current, start_date, end_date. silver_companies has load_timestamp.

-- Deduplicate source so only the LATEST record per company tries to CLOSE the existing Gold record
WITH latest_silver_companies AS (
    SELECT company_id, load_timestamp AS effective_date
    FROM __CATALOG__.__SCHEMA__.silver_companies
    WHERE batch_id = __BATCH_ID__
    QUALIFY ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY load_timestamp DESC) = 1
)
MERGE INTO __CATALOG__.__SCHEMA__.gold_dim_company AS target
USING latest_silver_companies AS source
ON target.company_id = source.company_id
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = source.effective_date,
    target.etl_timestamp = current_timestamp();

-- Step 2: Insert the new version (use load_timestamp for start_date, sector NULLs handled via COALESCE)
INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_company
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
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_companies sc
LEFT JOIN __CATALOG__.__SCHEMA__.silver_industry si ON sc.industry_id = si.in_id
WHERE sc.batch_id = __BATCH_ID__;

-- Optional: Z-Order by company_id for join performance when gold_dim_company is large
-- OPTIMIZE __CATALOG__.__SCHEMA__.gold_dim_company ZORDER BY (company_id)
