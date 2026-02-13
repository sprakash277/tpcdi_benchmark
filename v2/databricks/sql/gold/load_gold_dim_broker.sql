CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.gold_dim_broker AS
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
        split(raw_line, ',')[0] AS employee_id,
        split(raw_line, ',')[1] AS manager_id,
        split(raw_line, ',')[2] AS first_name,
        split(raw_line, ',')[3] AS last_name,
        split(raw_line, ',')[6] AS branch,
        split(raw_line, ',')[7] AS office,
        split(raw_line, ',')[8] AS phone
    FROM __CATALOG__.__SCHEMA__.bronze_hr
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND size(split(raw_line, ',')) >= 9
      AND split(raw_line, ',')[5] LIKE '%BROKER%'
) AS brokers
