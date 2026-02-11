# Databricks notebook source
# Load gold_dim_date from silver_date (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_dim_date AS
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
FROM {catalog}.{schema_name}.silver_date
WHERE batch_id = {batch_id}
""")
