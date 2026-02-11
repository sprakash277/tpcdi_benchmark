# Databricks notebook source
# MAGIC %md
# MAGIC # Create Gold Table: gold_fact_watches

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
# MAGIC -- TPC-DI v2: Gold Layer - Create gold_fact_watches
# MAGIC -- Set catalog and schema
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_fact_watches (
# MAGIC     sk_customer_id BIGINT NOT NULL,
# MAGIC     sk_security_id STRING NOT NULL,
# MAGIC     customer_id BIGINT NOT NULL,
# MAGIC     symbol STRING NOT NULL,
# MAGIC     watch_date TIMESTAMP NOT NULL,
# MAGIC     watch_action STRING,
# MAGIC     etl_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
