# Databricks notebook source
# MAGIC %md
# MAGIC # Create Gold Table: gold_fact_trade

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
# MAGIC -- TPC-DI v2: Gold Layer - Create gold_fact_trade
# MAGIC -- Set catalog and schema
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_fact_trade (
# MAGIC     sk_trade_id BIGINT NOT NULL,
# MAGIC     sk_date_id INT NOT NULL,
# MAGIC     sk_time_id INT,
# MAGIC     sk_customer_id BIGINT NOT NULL,
# MAGIC     sk_account_id BIGINT NOT NULL,
# MAGIC     sk_security_id STRING NOT NULL,
# MAGIC     sk_trade_type_id STRING NOT NULL,
# MAGIC     trade_id BIGINT NOT NULL,
# MAGIC     trade_dts TIMESTAMP NOT NULL,
# MAGIC     trade_price DOUBLE,
# MAGIC     trade_quantity INT,
# MAGIC     trade_amount DOUBLE,
# MAGIC     commission DOUBLE,
# MAGIC     charge DOUBLE,
# MAGIC     tax DOUBLE,
# MAGIC     status_id STRING,
# MAGIC     is_cash BOOLEAN,
# MAGIC     exec_name STRING,
# MAGIC     batch_id INT NOT NULL,
# MAGIC     late_arriving_flag BOOLEAN,  -- True if trade arrived before account/customer
# MAGIC     etl_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
