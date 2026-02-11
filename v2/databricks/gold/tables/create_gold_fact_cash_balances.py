# Databricks notebook source
# MAGIC %md
# MAGIC # Create Gold Table: gold_fact_cash_balances

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
# MAGIC -- TPC-DI v2: Gold Layer - Create gold_fact_cash_balances
# MAGIC -- Set catalog and schema
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_fact_cash_balances (
# MAGIC     sk_date_id INT NOT NULL,
# MAGIC     sk_account_id BIGINT NOT NULL,
# MAGIC     sk_customer_id BIGINT NOT NULL,
# MAGIC     account_id BIGINT NOT NULL,
# MAGIC     cash_balance DOUBLE,  -- Sum of CT_AMT by account/date
# MAGIC     transaction_count BIGINT,
# MAGIC     etl_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
