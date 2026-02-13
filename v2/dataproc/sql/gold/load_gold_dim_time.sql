DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_time;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_time AS
SELECT 
    sk_time_id AS sk_time_id,
    sk_time_id AS time_id,
    time_value,
    hour_id,
    hour_desc,
    minute_id,
    minute_desc,
    second_id,
    second_desc,
    market_hours_flag,
    office_hours_flag,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_time
WHERE batch_id = __BATCH_ID__
