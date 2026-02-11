# Databricks notebook source
# Load gold_prospect from silver_prospect (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_prospect AS
SELECT 
    agency_id,
    last_name,
    first_name,
    middle_initial,
    gender,
    address_line1,
    address_line2,
    postal_code,
    city,
    state,
    country,
    phone,
    income,
    number_cars,
    number_children,
    marital_status,
    age,
    credit_rating,
    own_or_rent_flag,
    employer,
    is_customer,
    net_worth,
    marketing_nameplate,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_prospect
WHERE batch_id = {batch_id}
""")
