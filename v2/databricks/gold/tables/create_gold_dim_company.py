# Databricks notebook source
# MAGIC %md
# MAGIC # Create Gold Table: gold_dim_company

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")

# Set SQL variables
spark.sql(f"SET var.catalog = '{catalog}'")
spark.sql(f"SET var.schema = '{schema_name}'")

# COMMAND ----------

# Set catalog and create/use schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------
# MAGIC %sql

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_dim_company (
# MAGIC     sk_company_id BIGINT NOT NULL,
# MAGIC     company_id STRING NOT NULL,  -- Natural key (CIK)
# MAGIC     company_name STRING,
# MAGIC     industry_id STRING,
# MAGIC     sector STRING,  -- Derived from industry
# MAGIC     status STRING,
# MAGIC     address_line1 STRING,
# MAGIC     address_line2 STRING,
# MAGIC     postal_code STRING,
# MAGIC     city STRING,
# MAGIC     state_prov STRING,
# MAGIC     country STRING,
# MAGIC     description STRING,
# MAGIC     founding_date DATE,
# MAGIC     ceo_name STRING,
# MAGIC     is_current BOOLEAN NOT NULL,
# MAGIC     etl_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
