DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_broker;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_broker USING DELTA AS
SELECT 
    monotonically_increasing_id() AS sk_broker_id,
    CAST(employee_id AS BIGINT) AS broker_id,
    CONCAT(first_name, ' ', last_name) AS broker_name,
    branch AS branch,
    office AS office,
    phone AS phone,
    TRUE AS is_current,
    current_timestamp() AS etl_timestamp
FROM (
    SELECT DISTINCT
        element_at(split(raw_line, ','), 1) AS employee_id,
        element_at(split(raw_line, ','), 2) AS manager_id,
        element_at(split(raw_line, ','), 3) AS first_name,
        element_at(split(raw_line, ','), 4) AS last_name,
        element_at(split(raw_line, ','), 7) AS branch,
        element_at(split(raw_line, ','), 8) AS office,
        element_at(split(raw_line, ','), 9) AS phone
    FROM __CATALOG__.__SCHEMA__.bronze_hr
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND size(split(raw_line, ',')) >= 9
      AND element_at(split(raw_line, ','), 6) LIKE '%BROKER%'
) AS brokers
