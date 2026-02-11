# Databricks notebook source
# Load gold_dim_company from silver_companies (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_dim_company AS
SELECT 
    sc.sk_company_id,
    sc.company_id,
    sc.company_name,
    sc.industry_id,
    si.in_sc_id AS sector,
    sc.status,
    sc.address_line1,
    sc.address_line2,
    sc.postal_code,
    sc.city,
    sc.state_province AS state_prov,
    sc.country,
    sc.description,
    sc.founding_date,
    sc.ceo_name,
    TRUE AS is_current,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_companies sc
LEFT JOIN {catalog}.{schema_name}.silver_industry si ON sc.industry_id = si.in_id
WHERE sc.batch_id = {batch_id}
""")
