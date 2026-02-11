# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table: silver_customers

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
# MAGIC CREATE TABLE IF NOT EXISTS silver_customers (
# MAGIC     sk_customer_id BIGINT,
# MAGIC     customer_id BIGINT NOT NULL,
# MAGIC     tax_id STRING,
# MAGIC     status STRING,
# MAGIC     last_name STRING,
# MAGIC     first_name STRING,
# MAGIC     middle_name STRING,
# MAGIC     gender STRING,
# MAGIC     tier INT,
# MAGIC     dob DATE,
# MAGIC     address_line1 STRING,
# MAGIC     address_line2 STRING,
# MAGIC     postal_code STRING,
# MAGIC     city STRING,
# MAGIC     state_prov STRING,
# MAGIC     country STRING,
# MAGIC     email1 STRING,
# MAGIC     email2 STRING,
# MAGIC     local_tax_id STRING,
# MAGIC     national_tax_id STRING,
# MAGIC     is_current BOOLEAN NOT NULL,
# MAGIC     effective_date TIMESTAMP NOT NULL,
# MAGIC     end_date TIMESTAMP,
# MAGIC     batch_id INT NOT NULL,
# MAGIC     load_timestamp TIMESTAMP NOT NULL,
# MAGIC     record_type STRING  -- I=Insert, U=Update, D=Delete
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
