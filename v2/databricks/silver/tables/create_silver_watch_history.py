# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table: silver_watch_history

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
# MAGIC CREATE TABLE IF NOT EXISTS silver_watch_history (
# MAGIC     wh_key STRING NOT NULL,  -- Composite: w_c_id + w_s_symb
# MAGIC     w_c_id BIGINT NOT NULL,  -- Customer ID
# MAGIC     w_s_symb STRING NOT NULL,  -- Security symbol
# MAGIC     w_dts TIMESTAMP NOT NULL,
# MAGIC     w_action STRING,
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
