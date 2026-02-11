# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Batch Pipeline (Single Entry from Workflow)
# MAGIC
# MAGIC One notebook: widgets here, then Bronze → Silver → Gold. SQL in `sql/` files; Python-only steps run via sub-notebooks.

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw Data Path")
dbutils.widgets.text("sf", "10", "Scale Factor")
dbutils.widgets.text("batch_id", "1", "Batch ID")
dbutils.widgets.text("xml_format", "com.databricks.spark.xml", "XML Format")
dbutils.widgets.text("sql_base_path", "", "SQL base path (optional; default = notebook dir)")

# COMMAND ----------

import os

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = dbutils.widgets.get("batch_id")
xml_format = dbutils.widgets.get("xml_format") or "com.databricks.spark.xml"
full_raw_data_path = f"{raw_data_path}/sf={sf}"
sql_base_path = dbutils.widgets.get("sql_base_path") or ""

if sql_base_path:
    base_dir = sql_base_path.rstrip("/")
else:
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        base_dir = os.path.dirname(notebook_path)
    except Exception:
        base_dir = ""
    if not base_dir and "__file__" in dir():
        base_dir = os.path.dirname(os.path.abspath(__file__))

def _workspace_file_path(path):
    """Convert path to workspace file URI so dbutils.fs and Spark can read it."""
    if path.startswith("/Users/") and not path.startswith("/Workspace/"):
        return "file:/Workspace" + path
    if path.startswith("/Repos/") and not path.startswith("file:"):
        return "file:" + path
    return path

def read_sql_file(rel_path):
    path = os.path.join(base_dir, rel_path) if base_dir else rel_path
    # Use workspace file URI when path is under /Users/ or /Repos/ (notebook context)
    read_path = _workspace_file_path(path)
    try:
        return dbutils.fs.head(read_path)
    except Exception:
        try:
            return "".join([r[0] for r in spark.read.text(read_path).collect()])
        except Exception:
            try:
                # Fallback: local path when running with WSFS (e.g. /Workspace/Users/...)
                local_path = path if path.startswith("/Workspace/") else ("/Workspace" + path if path.startswith("/Users/") else path)
                with open(local_path, "r") as f:
                    return f.read()
            except Exception as e:
                raise FileNotFoundError(f"Cannot read SQL file: {path} (tried {read_path})") from e

def run_sql(sql_content, use_pipe_placeholder=False):
    s = sql_content.replace("__CATALOG__", catalog).replace("__SCHEMA__", schema_name)
    s = s.replace("__RAW_DATA_PATH__", full_raw_data_path).replace("__BATCH_ID__", str(batch_id))
    if use_pipe_placeholder:
        s = s.replace("__PIPE__", "\\|")
    spark.sql(s)

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

bronze_sql_before_finwire = [
    "sql/bronze/load_bronze_date.sql",
    "sql/bronze/load_bronze_time.sql",
    "sql/bronze/load_bronze_status_type.sql",
    "sql/bronze/load_bronze_trade_type.sql",
    "sql/bronze/load_bronze_industry.sql",
    "sql/bronze/load_bronze_tax_rate.sql",
]
for rel in bronze_sql_before_finwire:
    print(f"Bronze SQL: {rel}")
    run_sql(read_sql_file(rel))

params = {"catalog": catalog, "schema_name": schema_name, "raw_data_path": raw_data_path, "sf": sf, "batch_id": batch_id, "xml_format": xml_format}
bronze_notebook_dir = (base_dir + "/bronze/batch") if base_dir else "bronze/batch"
print("Bronze: customer_mgmt")
dbutils.notebook.run(bronze_notebook_dir + "/load_bronze_customer_mgmt", timeout_seconds=600, arguments=params)
print("Bronze: finwire")
dbutils.notebook.run(bronze_notebook_dir + "/load_bronze_finwire", timeout_seconds=600, arguments=params)

bronze_sql_after_finwire = [
    "sql/bronze/load_bronze_trade.sql",
    "sql/bronze/load_bronze_daily_market.sql",
    "sql/bronze/load_bronze_cash_transaction.sql",
    "sql/bronze/load_bronze_holding_history.sql",
    "sql/bronze/load_bronze_watch_history.sql",
    "sql/bronze/load_bronze_hr.sql",
    "sql/bronze/load_bronze_prospect.sql",
]
for rel in bronze_sql_after_finwire:
    print(f"Bronze SQL: {rel}")
    run_sql(read_sql_file(rel))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

silver_sql_files = [
    "sql/silver/transform_silver_date.sql",
    "sql/silver/transform_silver_time.sql",
    "sql/silver/transform_silver_status_type.sql",
    "sql/silver/transform_silver_trade_type.sql",
    "sql/silver/transform_silver_industry.sql",
    "sql/silver/transform_silver_tax_rate.sql",
    "sql/silver/transform_silver_companies.sql",
    "sql/silver/transform_silver_securities.sql",
    "sql/silver/transform_silver_financials.sql",
    "sql/silver/transform_silver_trades.sql",
    "sql/silver/transform_silver_daily_market.sql",
    "sql/silver/transform_silver_cash_transaction.sql",
    "sql/silver/transform_silver_holding_history.sql",
    "sql/silver/transform_silver_watch_history.sql",
    "sql/silver/transform_silver_prospect.sql",
]
for rel in silver_sql_files:
    print(f"Silver SQL: {rel}")
    sql_content = read_sql_file(rel)
    run_sql(sql_content, use_pipe_placeholder=True)

# COMMAND ----------

silver_notebook_dir = base_dir + "/silver/batch" if base_dir else "silver/batch"
print("Silver: customers")
dbutils.notebook.run(silver_notebook_dir + "/transform_silver_customers", timeout_seconds=600, arguments=params)
print("Silver: accounts")
dbutils.notebook.run(silver_notebook_dir + "/transform_silver_accounts", timeout_seconds=600, arguments=params)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

gold_sql_files = [
    "sql/gold/load_gold_dim_date.sql",
    "sql/gold/load_gold_dim_time.sql",
    "sql/gold/load_gold_dim_status_type.sql",
    "sql/gold/load_gold_dim_trade_type.sql",
    "sql/gold/load_gold_dim_industry.sql",
    "sql/gold/load_gold_dim_account.sql",
    "sql/gold/load_gold_dim_customer.sql",
    "sql/gold/load_gold_dim_broker.sql",
    "sql/gold/load_gold_dim_company.sql",
    "sql/gold/load_gold_dim_security.sql",
    "sql/gold/load_gold_fact_trade.sql",
    "sql/gold/load_gold_fact_cash_balances.sql",
    "sql/gold/load_gold_fact_holdings.sql",
    "sql/gold/load_gold_fact_market_history.sql",
    "sql/gold/load_gold_fact_watches.sql",
    "sql/gold/load_gold_financials.sql",
    "sql/gold/load_gold_prospect.sql",
]
for rel in gold_sql_files:
    print(f"Gold SQL: {rel}")
    sql_content = read_sql_file(rel)
    run_sql(sql_content)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
