DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_industry;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_industry AS
SELECT 
    in_id AS sk_industry_id,
    in_id AS industry_id,
    in_name AS industry_name,
    in_sc_id AS sector_id,
    NULL AS sector_name,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_industry
WHERE batch_id = __BATCH_ID__
