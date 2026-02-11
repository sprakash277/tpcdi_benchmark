# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table: silver_daily_market

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
# MAGIC CREATE TABLE IF NOT EXISTS silver_daily_market (
# MAGIC     dm_key STRING NOT NULL,  -- Composite: dm_date + dm_s_symb
# MAGIC     dm_date DATE NOT NULL,
# MAGIC     dm_s_symb STRING NOT NULL,
# MAGIC     dm_close DOUBLE,
# MAGIC     dm_high DOUBLE,
# MAGIC     dm_low DOUBLE,
# MAGIC     dm_vol BIGINT,
# MAGIC     batch_id INT NOT NULL,
# MAGIC     load_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
