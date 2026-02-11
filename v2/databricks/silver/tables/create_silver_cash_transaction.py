# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table: silver_cash_transaction

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
# MAGIC CREATE TABLE IF NOT EXISTS silver_cash_transaction (
# MAGIC     ct_key STRING NOT NULL,  -- Composite: ct_ca_id + ct_dts
# MAGIC     ct_ca_id BIGINT NOT NULL,  -- Account ID
# MAGIC     ct_dts TIMESTAMP NOT NULL,
# MAGIC     ct_amt DOUBLE,
# MAGIC     ct_name STRING,
# MAGIC     is_current BOOLEAN NOT NULL,
# MAGIC     effective_date TIMESTAMP NOT NULL,
# MAGIC     end_date TIMESTAMP,
# MAGIC     batch_id INT NOT NULL,
# MAGIC     load_timestamp TIMESTAMP NOT NULL,
# MAGIC     record_type STRING
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
