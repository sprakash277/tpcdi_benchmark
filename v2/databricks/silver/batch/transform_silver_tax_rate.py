# Databricks notebook source
# Transform bronze_tax_rate -> silver_tax_rate (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_tax_rate AS
SELECT 
    split(raw_line, '\\\\|')[0] AS tx_id,
    split(raw_line, '\\\\|')[1] AS tx_name,
    CAST(split(raw_line, '\\\\|')[2] AS DOUBLE) AS tx_rate,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_tax_rate
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
""")
