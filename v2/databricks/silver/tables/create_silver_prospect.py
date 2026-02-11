# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table: silver_prospect

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
# MAGIC CREATE TABLE IF NOT EXISTS silver_prospect (
# MAGIC     agency_id STRING NOT NULL,
# MAGIC     last_name STRING,
# MAGIC     first_name STRING,
# MAGIC     middle_initial STRING,
# MAGIC     gender STRING,
# MAGIC     address_line1 STRING,
# MAGIC     address_line2 STRING,
# MAGIC     postal_code STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     country STRING,
# MAGIC     phone STRING,
# MAGIC     income INT,
# MAGIC     number_cars INT,
# MAGIC     number_children INT,
# MAGIC     marital_status STRING,
# MAGIC     age INT,
# MAGIC     credit_rating INT,
# MAGIC     own_or_rent_flag STRING,
# MAGIC     employer STRING,
# MAGIC     is_customer BOOLEAN,
# MAGIC     net_worth BIGINT,
# MAGIC     marketing_nameplate STRING,
# MAGIC     batch_id INT NOT NULL,
# MAGIC     load_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
