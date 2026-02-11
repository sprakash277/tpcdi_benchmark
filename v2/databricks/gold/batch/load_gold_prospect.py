# Databricks notebook source
# Load gold_prospect from silver_prospect
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

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
