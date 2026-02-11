# Databricks notebook source
# Load gold_fact_market_history from silver_daily_market
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.gold_fact_market_history AS
SELECT 
    dd.sk_date_id,
    ds.sk_security_id,
    dc.sk_company_id,
    sdm.dm_date AS market_date,
    sdm.dm_s_symb AS symbol,
    sdm.dm_close AS close_price,
    sdm.dm_high AS high_price,
    sdm.dm_low AS low_price,
    sdm.dm_vol AS volume,
    sdm.batch_id,
    current_timestamp() AS etl_timestamp
FROM {catalog}.{schema_name}.silver_daily_market sdm
INNER JOIN {catalog}.{schema_name}.gold_dim_date dd ON sdm.dm_date = dd.date_value
INNER JOIN {catalog}.{schema_name}.gold_dim_security ds ON sdm.dm_s_symb = ds.symbol
LEFT JOIN {catalog}.{schema_name}.gold_dim_company dc ON ds.company_id = dc.company_id
WHERE sdm.batch_id = {batch_id}
""")
