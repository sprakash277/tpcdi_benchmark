# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table: silver_securities

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
# MAGIC %sql
# MAGIC -- TPC-DI v2: Silver Layer - Create silver_securities
# MAGIC -- Set catalog and schema
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_securities (
# MAGIC     symbol STRING NOT NULL,
# MAGIC     issue_type STRING,
# MAGIC     status STRING,
# MAGIC     name STRING,
# MAGIC     ex_id STRING,
# MAGIC     sh_out BIGINT,
# MAGIC     first_trade_date DATE,
# MAGIC     first_trade_exchg STRING,
# MAGIC     dividend DOUBLE,
# MAGIC     co_name_or_cik STRING,  -- Company reference
# MAGIC     batch_id INT NOT NULL,
# MAGIC     load_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
