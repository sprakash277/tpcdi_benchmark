CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.gold_dim_date AS
SELECT 
    sk_date_id AS sk_date_id,
    sk_date_id AS date_id,
    date_value,
    date_desc,
    calendar_year_id,
    calendar_year_desc,
    calendar_qtr_id,
    calendar_qtr_desc,
    calendar_month_id,
    calendar_month_desc,
    calendar_week_id,
    calendar_week_desc,
    day_of_week_num,
    day_of_week_desc,
    fiscal_year_id,
    fiscal_year_desc,
    fiscal_qtr_id,
    fiscal_qtr_desc,
    holiday_flag,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_date
WHERE batch_id = __BATCH_ID__
