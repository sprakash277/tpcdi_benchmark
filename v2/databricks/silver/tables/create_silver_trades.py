# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table: silver_trades

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
# MAGIC -- TPC-DI v2: Silver Layer - Create silver_trades
# MAGIC -- Set catalog and schema
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_trades (
# MAGIC     trade_id BIGINT NOT NULL,
# MAGIC     trade_dts TIMESTAMP NOT NULL,
# MAGIC     status_id STRING,
# MAGIC     trade_type_id STRING,
# MAGIC     is_cash BOOLEAN,
# MAGIC     symbol STRING,
# MAGIC     quantity INT,
# MAGIC     bid_price DOUBLE,
# MAGIC     account_id BIGINT NOT NULL,
# MAGIC     exec_name STRING,
# MAGIC     trade_price DOUBLE,
# MAGIC     charge DOUBLE,
# MAGIC     commission DOUBLE,
# MAGIC     tax DOUBLE,
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
