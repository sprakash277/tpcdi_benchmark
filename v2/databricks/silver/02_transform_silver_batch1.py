# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Bronze to Silver (Batch 1)
# MAGIC
# MAGIC Orchestrator: runs one notebook per silver table (batch/transform_silver_*.py).

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

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

def get_silver_batch_notebooks():
    return [
        "batch/transform_silver_date",
        "batch/transform_silver_time",
        "batch/transform_silver_status_type",
        "batch/transform_silver_trade_type",
        "batch/transform_silver_industry",
        "batch/transform_silver_tax_rate",
        "batch/transform_silver_companies",
        "batch/transform_silver_securities",
        "batch/transform_silver_financials",
        "batch/transform_silver_customers",
        "batch/transform_silver_accounts",
        "batch/transform_silver_trades",
        "batch/transform_silver_daily_market",
        "batch/transform_silver_cash_transaction",
        "batch/transform_silver_holding_history",
        "batch/transform_silver_watch_history",
        "batch/transform_silver_prospect",
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

for rel_path in get_silver_batch_notebooks():
    run_path = f"{base_path}/{rel_path}" if base_path else rel_path
    print(f"Running {run_path} ...")
    dbutils.notebook.run(run_path, timeout_seconds=600, arguments=params)
    print(f"Done: {run_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

spark.sql("SELECT 'Load completed' AS status").show()
