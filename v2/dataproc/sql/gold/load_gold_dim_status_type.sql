DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_status_type;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_status_type USING DELTA AS
SELECT 
    st_id AS sk_status_type_id,
    st_id AS status_type_id,
    st_id AS status_type_code,
    st_name AS status_type_name,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_status_type
WHERE batch_id = __BATCH_ID__
