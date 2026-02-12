DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_broker;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_broker AS
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
        split(raw_line, ',')[4] AS branch,
        split(raw_line, ',')[5] AS office,
        split(raw_line, ',')[6] AS phone,
        split(raw_line, ',')[7] AS job_code
    FROM __CATALOG__.__SCHEMA__.bronze_hr
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND split(raw_line, ',')[7] LIKE '%BROKER%'
) AS brokers
