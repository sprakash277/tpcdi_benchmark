# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table: silver_companies

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
# MAGIC CREATE TABLE IF NOT EXISTS silver_companies (
# MAGIC     sk_company_id BIGINT,
# MAGIC     company_id STRING NOT NULL,  -- CIK
# MAGIC     company_name STRING,
# MAGIC     industry_id STRING,
# MAGIC     sp_rating STRING,
# MAGIC     status STRING,
# MAGIC     founding_date DATE,
# MAGIC     ceo_name STRING,
# MAGIC     address_line1 STRING,
# MAGIC     address_line2 STRING,
# MAGIC     postal_code STRING,
# MAGIC     city STRING,
# MAGIC     state_province STRING,
# MAGIC     country STRING,
# MAGIC     description STRING,
# MAGIC     batch_id INT NOT NULL,
# MAGIC     load_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
