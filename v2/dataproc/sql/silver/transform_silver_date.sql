DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.silver_date;
CREATE TABLE __CATALOG__.__SCHEMA__.silver_date AS
SELECT 
    CAST(split_part(raw_line, '|', 1) AS INT) AS sk_date_id,
    try_cast(split_part(raw_line, '|', 2) AS DATE) AS date_value,
    split_part(raw_line, '|', 3) AS date_desc,
    CAST(split_part(raw_line, '|', 4) AS INT) AS calendar_year_id,
    split_part(raw_line, '|', 5) AS calendar_year_desc,
    CAST(split_part(raw_line, '|', 6) AS INT) AS calendar_qtr_id,
    split_part(raw_line, '|', 7) AS calendar_qtr_desc,
    CAST(split_part(raw_line, '|', 8) AS INT) AS calendar_month_id,
    split_part(raw_line, '|', 9) AS calendar_month_desc,
    CAST(split_part(raw_line, '|', 10) AS INT) AS calendar_week_id,
    split_part(raw_line, '|', 11) AS calendar_week_desc,
    CAST(split_part(raw_line, '|', 12) AS INT) AS day_of_week_num,
    split_part(raw_line, '|', 13) AS day_of_week_desc,
    CAST(split_part(raw_line, '|', 14) AS INT) AS fiscal_year_id,
    split_part(raw_line, '|', 15) AS fiscal_year_desc,
    CAST(split_part(raw_line, '|', 16) AS INT) AS fiscal_qtr_id,
    split_part(raw_line, '|', 17) AS fiscal_qtr_desc,
    try_cast(split_part(raw_line, '|', 18) AS BOOLEAN) AS holiday_flag,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_date
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
