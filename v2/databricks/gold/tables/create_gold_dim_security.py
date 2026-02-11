# Databricks notebook source
# MAGIC %md
# MAGIC # Create Gold Table: gold_dim_security

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
# MAGIC CREATE TABLE IF NOT EXISTS gold_dim_security (
# MAGIC     sk_security_id STRING NOT NULL,  -- Natural key (Symbol)
# MAGIC     security_id STRING NOT NULL,  -- Same as symbol
# MAGIC     symbol STRING NOT NULL,
# MAGIC     issue_type STRING,
# MAGIC     status STRING,
# MAGIC     name STRING,
# MAGIC     exchange_id STRING,
# MAGIC     shares_outstanding BIGINT,
# MAGIC     first_trade_date DATE,
# MAGIC     first_trade_exchange STRING,
# MAGIC     dividend DOUBLE,
# MAGIC     company_id STRING,  -- Reference to DimCompany
# MAGIC     is_current BOOLEAN NOT NULL,
# MAGIC     etl_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
