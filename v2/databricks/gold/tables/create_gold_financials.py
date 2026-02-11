# Databricks notebook source
# MAGIC %md
# MAGIC # Create Gold Table: gold_financials

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
# MAGIC -- TPC-DI v2: Gold Layer - Create gold_financials
# MAGIC -- Set catalog and schema
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_financials (
# MAGIC     co_name_or_cik STRING NOT NULL,
# MAGIC     year INT NOT NULL,
# MAGIC     quarter INT NOT NULL,
# MAGIC     qtr_start_date DATE,
# MAGIC     posting_date DATE,
# MAGIC     revenue DOUBLE,
# MAGIC     earnings DOUBLE,
# MAGIC     eps DOUBLE,
# MAGIC     diluted_eps DOUBLE,
# MAGIC     margin DOUBLE,
# MAGIC     inventory DOUBLE,
# MAGIC     assets DOUBLE,
# MAGIC     liabilities DOUBLE,
# MAGIC     sh_out BIGINT,
# MAGIC     diluted_sh_out BIGINT,
# MAGIC     etl_timestamp TIMESTAMP NOT NULL,
# MAGIC     PRIMARY KEY (co_name_or_cik, year, quarter)
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
