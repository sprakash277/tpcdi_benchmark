# Databricks notebook source
# Transform bronze_date -> silver_date (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_date AS
SELECT 
    CAST(split(raw_line, '\\\\|')[0] AS INT) AS sk_date_id,
    CAST(split(raw_line, '\\\\|')[1] AS DATE) AS date_value,
    split(raw_line, '\\\\|')[2] AS date_desc,
    CAST(split(raw_line, '\\\\|')[3] AS INT) AS calendar_year_id,
    split(raw_line, '\\\\|')[4] AS calendar_year_desc,
    CAST(split(raw_line, '\\\\|')[5] AS INT) AS calendar_qtr_id,
    split(raw_line, '\\\\|')[6] AS calendar_qtr_desc,
    CAST(split(raw_line, '\\\\|')[7] AS INT) AS calendar_month_id,
    split(raw_line, '\\\\|')[8] AS calendar_month_desc,
    CAST(split(raw_line, '\\\\|')[9] AS INT) AS calendar_week_id,
    split(raw_line, '\\\\|')[10] AS calendar_week_desc,
    CAST(split(raw_line, '\\\\|')[11] AS INT) AS day_of_week_num,
    split(raw_line, '\\\\|')[12] AS day_of_week_desc,
    CAST(split(raw_line, '\\\\|')[13] AS INT) AS fiscal_year_id,
    split(raw_line, '\\\\|')[14] AS fiscal_year_desc,
    CAST(split(raw_line, '\\\\|')[15] AS INT) AS fiscal_qtr_id,
    split(raw_line, '\\\\|')[16] AS fiscal_qtr_desc,
    CAST(split(raw_line, '\\\\|')[17] AS BOOLEAN) AS holiday_flag,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_date
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
""")
