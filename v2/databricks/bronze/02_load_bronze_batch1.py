# Databricks notebook source
# MAGIC %md
# MAGIC # Load Bronze Batch 1 Data
# MAGIC
# MAGIC Orchestrator: runs one notebook per bronze table (batch/load_bronze_*.py).

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw Data Path")
dbutils.widgets.text("sf", "10", "Scale Factor")
dbutils.widgets.text("batch_id", "1", "Batch ID")
dbutils.widgets.text("xml_format", "com.databricks.spark.xml", "XML Format")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = dbutils.widgets.get("batch_id")
xml_format = dbutils.widgets.get("xml_format") or "com.databricks.spark.xml"

# COMMAND ----------

# Set catalog and create/use schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

# Run one notebook per bronze table (same order as before)
def get_bronze_batch_notebooks():
    return [
        "batch/load_bronze_date",
        "batch/load_bronze_time",
        "batch/load_bronze_status_type",
        "batch/load_bronze_trade_type",
        "batch/load_bronze_industry",
        "batch/load_bronze_tax_rate",
        "batch/load_bronze_customer_mgmt",
        "batch/load_bronze_finwire",
        "batch/load_bronze_trade",
        "batch/load_bronze_daily_market",
        "batch/load_bronze_cash_transaction",
        "batch/load_bronze_holding_history",
        "batch/load_bronze_watch_history",
        "batch/load_bronze_hr",
        "batch/load_bronze_prospect",
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

for rel_path in get_bronze_batch_notebooks():
    run_path = f"{base_path}/{rel_path}" if base_path else rel_path
    print(f"Running {run_path} ...")
    dbutils.notebook.run(run_path, timeout_seconds=600, arguments=params)
    print(f"Done: {run_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

spark.sql(f"""
SELECT 
    'bronze_date' AS table_name,
    COUNT(*) AS row_count
FROM {catalog}.{schema_name}.bronze_date
WHERE _batch_id = {batch_id}
UNION ALL
SELECT 'bronze_trade', COUNT(*) FROM {catalog}.{schema_name}.bronze_trade WHERE _batch_id = {batch_id}
UNION ALL
SELECT 'bronze_daily_market', COUNT(*) FROM {catalog}.{schema_name}.bronze_daily_market WHERE _batch_id = {batch_id}
UNION ALL
SELECT 'bronze_finwire', COUNT(*) FROM {catalog}.{schema_name}.bronze_finwire WHERE _batch_id = {batch_id}
""").show()
