# Databricks notebook source
# MAGIC %md
# MAGIC # Create Gold Table: gold_dim_messages

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
# MAGIC -- TPC-DI v2: Gold Layer - Create gold_dim_messages
# MAGIC -- Set catalog and schema
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_dim_messages (
# MAGIC     message_timestamp TIMESTAMP NOT NULL,
# MAGIC     batch_id INT NOT NULL,
# MAGIC     originating_table STRING NOT NULL,  -- Source table (e.g., 'FactTrade', 'DimCustomer')
# MAGIC     message_text STRING NOT NULL,
# MAGIC     message_type STRING NOT NULL,  -- 'Alert', 'Reject', 'Info'
# MAGIC     component_name STRING,  -- Component that generated the message (e.g., 'Silver_Customer_Validation')
# MAGIC     severity STRING  -- 'Alert', 'Reject', 'Warning', 'Info'
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC     'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------
