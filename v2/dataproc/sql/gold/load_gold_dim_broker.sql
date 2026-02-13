DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_dim_broker;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_dim_broker
USING DELTA
-- Use Liquid Clustering for performance on Dataproc 2.3
CLUSTER BY (broker_id) AS
SELECT
    ROW_NUMBER() OVER (ORDER BY employee_id) AS sk_broker_id,
    CAST(employee_id AS BIGINT) AS broker_id,
    CAST(manager_id AS BIGINT) AS manager_id,
    first_name,
    last_name,
    middle_initial,
    branch,
    office,
    phone,
    true AS is_current,
    __BATCH_ID__ AS batch_id,
    (SELECT MIN(date_value) FROM __CATALOG__.__SCHEMA__.gold_dim_date) AS start_date,
    CAST('9999-12-31' AS DATE) AS end_date,
    current_timestamp() AS etl_timestamp
FROM (
    SELECT DISTINCT
        TRIM(element_at(split(raw_line, ','), 1)) AS employee_id,
        TRIM(element_at(split(raw_line, ','), 2)) AS manager_id,
        TRIM(element_at(split(raw_line, ','), 3)) AS first_name,
        TRIM(element_at(split(raw_line, ','), 4)) AS last_name,
        TRIM(element_at(split(raw_line, ','), 5)) AS middle_initial,
        TRIM(element_at(split(raw_line, ','), 7)) AS branch,
        TRIM(element_at(split(raw_line, ','), 8)) AS office,
        TRIM(element_at(split(raw_line, ','), 9)) AS phone
    FROM __CATALOG__.__SCHEMA__.bronze_hr
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND size(split(raw_line, ',')) >= 9
      AND (
          TRIM(element_at(split(raw_line, ','), 6)) = '314'
          OR LOWER(TRIM(element_at(split(raw_line, ','), 6))) LIKE '%broker%'
          OR TRIM(element_at(split(raw_line, ','), 6)) = '1'
      )
) AS brokers
