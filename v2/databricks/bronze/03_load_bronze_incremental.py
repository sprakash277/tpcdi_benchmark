# Databricks notebook source
# MAGIC %md
# MAGIC # Load Bronze Incremental Data
# MAGIC
# MAGIC Orchestrator: creates bronze tables if needed, then runs one notebook per incremental bronze table (incremental/load_bronze_*.py).

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw Data Path")
dbutils.widgets.text("sf", "10", "Scale Factor")
dbutils.widgets.text("batch_id", "2", "Batch ID")
dbutils.widgets.text("xml_format", "com.databricks.spark.xml", "XML Format")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = dbutils.widgets.get("batch_id")
xml_format = dbutils.widgets.get("xml_format") or "com.databricks.spark.xml"
full_raw_data_path = f"{raw_data_path}/sf={sf}"

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Tables (if not exist)
# MAGIC

# COMMAND ----------

import os
try:
    current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = os.path.dirname(current_notebook_path)
except Exception:
    base_path = ""
tables_path = f"{base_path}/tables" if base_path else "tables"

bronze_tables = [
    "bronze_date", "bronze_time", "bronze_status_type", "bronze_trade_type",
    "bronze_industry", "bronze_tax_rate", "bronze_hr", "bronze_customer_mgmt",
    "bronze_customer", "bronze_account", "bronze_trade", "bronze_daily_market",
    "bronze_prospect", "bronze_cash_transaction", "bronze_holding_history",
    "bronze_watch_history", "bronze_finwire",
]

for table_name in bronze_tables:
    create_notebook = f"{tables_path}/create_{table_name}"
    print(f"Creating table: {table_name} via {create_notebook}")
    try:
        dbutils.notebook.run(create_notebook, timeout_seconds=300, arguments={
            "catalog": catalog,
            "schema_name": schema_name
        })
    except Exception as e:
        if "already exists" not in str(e).lower() and "table" not in str(e).lower():
            print(f"Warning: Error creating {table_name}: {e}")
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run incremental load per table

# COMMAND ----------

def get_bronze_incremental_notebooks():
    return [
        "incremental/load_bronze_customer",
        "incremental/load_bronze_account",
        "incremental/load_bronze_trade",
        "incremental/load_bronze_daily_market",
        "incremental/load_bronze_cash_transaction",
        "incremental/load_bronze_holding_history",
        "incremental/load_bronze_watch_history",
        "incremental/load_bronze_prospect",
    ]

params = {
    "catalog": catalog,
    "schema_name": schema_name,
    "raw_data_path": raw_data_path,
    "sf": sf,
    "batch_id": batch_id,
    "xml_format": xml_format,
}

try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    base_path = "/".join(notebook_path.rstrip("/").split("/")[:-1])
except Exception:
    base_path = ""

for rel_path in get_bronze_incremental_notebooks():
    run_path = f"{base_path}/{rel_path}" if base_path else rel_path
    print(f"Running {run_path} ...")
    dbutils.notebook.run(run_path, timeout_seconds=600, arguments=params)
    print(f"Done: {run_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

spark.sql("SELECT 'Load completed' AS status").show()
