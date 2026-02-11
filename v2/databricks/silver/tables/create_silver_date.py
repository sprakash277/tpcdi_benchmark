# Databricks notebook source
# MAGIC %md
# MAGIC # Create Silver Table: silver_date

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
# MAGIC -- TPC-DI v2: Silver Layer - Create silver_date
# MAGIC -- Set catalog and schema
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_date (
# MAGIC     sk_date_id INT NOT NULL,
# MAGIC     date_value DATE NOT NULL,
# MAGIC     date_desc STRING,
# MAGIC     calendar_year_id INT,
# MAGIC     calendar_year_desc STRING,
# MAGIC     calendar_qtr_id INT,
# MAGIC     calendar_qtr_desc STRING,
# MAGIC     calendar_month_id INT,
# MAGIC     calendar_month_desc STRING,
# MAGIC     calendar_week_id INT,
# MAGIC     calendar_week_desc STRING,
# MAGIC     day_of_week_num INT,
# MAGIC     day_of_week_desc STRING,
# MAGIC     fiscal_year_id INT,
# MAGIC     fiscal_year_desc STRING,
# MAGIC     fiscal_qtr_id INT,
# MAGIC     fiscal_qtr_desc STRING,
# MAGIC     holiday_flag BOOLEAN,
# MAGIC     batch_id INT NOT NULL,
# MAGIC     load_timestamp TIMESTAMP NOT NULL
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
