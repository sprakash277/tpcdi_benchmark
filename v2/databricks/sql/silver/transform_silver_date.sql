CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_date AS
SELECT 
    CAST(split(raw_line, '__PIPE__')[0] AS INT) AS sk_date_id,
    CAST(split(raw_line, '__PIPE__')[1] AS DATE) AS date_value,
    split(raw_line, '__PIPE__')[2] AS date_desc,
    CAST(split(raw_line, '__PIPE__')[3] AS INT) AS calendar_year_id,
    split(raw_line, '__PIPE__')[4] AS calendar_year_desc,
    CAST(split(raw_line, '__PIPE__')[5] AS INT) AS calendar_qtr_id,
    split(raw_line, '__PIPE__')[6] AS calendar_qtr_desc,
    CAST(split(raw_line, '__PIPE__')[7] AS INT) AS calendar_month_id,
    split(raw_line, '__PIPE__')[8] AS calendar_month_desc,
    CAST(split(raw_line, '__PIPE__')[9] AS INT) AS calendar_week_id,
    split(raw_line, '__PIPE__')[10] AS calendar_week_desc,
    CAST(split(raw_line, '__PIPE__')[11] AS INT) AS day_of_week_num,
    split(raw_line, '__PIPE__')[12] AS day_of_week_desc,
    CAST(split(raw_line, '__PIPE__')[13] AS INT) AS fiscal_year_id,
    split(raw_line, '__PIPE__')[14] AS fiscal_year_desc,
    CAST(split(raw_line, '__PIPE__')[15] AS INT) AS fiscal_qtr_id,
    split(raw_line, '__PIPE__')[16] AS fiscal_qtr_desc,
    CAST(split(raw_line, '__PIPE__')[17] AS BOOLEAN) AS holiday_flag,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_date
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
