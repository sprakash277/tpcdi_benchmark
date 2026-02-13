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
dbutils.widgets.dropdown("load_type", "batch", ["batch", "incremental"], "Load Type (batch = full load, incremental = batch 2+)")
dbutils.widgets.text("xml_format", "com.databricks.spark.xml", "XML Format")
dbutils.widgets.text("sql_base_path", "", "SQL base path (optional; default = notebook dir)")
dbutils.widgets.text("metrics_output", "gs://sumit_prakash_gcs/tpcdi/metrics", "Metrics Output Path")

# COMMAND ----------

import os
import sys
import time

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = dbutils.widgets.get("batch_id")
load_type = dbutils.widgets.get("load_type") or "batch"
xml_format = dbutils.widgets.get("xml_format") or "com.databricks.spark.xml"
metrics_output = (dbutils.widgets.get("metrics_output") or "").strip()
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
if base_dir and base_dir not in sys.path:
    sys.path.insert(0, base_dir)
import tpcdi_metrics as metrics

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

def run_sql_multi(sql_content, use_pipe_placeholder=False):
    """Replace placeholders and run each statement (split by semicolon)."""
    s = sql_content.replace("__CATALOG__", catalog).replace("__SCHEMA__", schema_name)
    s = s.replace("__RAW_DATA_PATH__", full_raw_data_path).replace("__BATCH_ID__", str(batch_id))
    if use_pipe_placeholder:
        s = s.replace("__PIPE__", "\\|")
    for stmt in s.split(";"):
        stmt = stmt.strip()
        if stmt and not all(line.strip().startswith("--") or not line.strip() for line in stmt.split("\n")):
            spark.sql(stmt)

# --- Timed runners call into tpcdi_metrics for stats
def run_sql_timed(rel_path, use_pipe_placeholder=False):
    """Run single-statement SQL file and record duration + table stats. Uses REFRESH for batch so stats see current state."""
    global _total_refresh_seconds
    table_name = metrics.sql_file_to_table_name(rel_path)
    t0 = time.time()
    run_sql(read_sql_file(rel_path), use_pipe_placeholder=use_pipe_placeholder)
    duration = time.time() - t0
    if table_name:
        use_refresh = not is_incremental
        row_count, size_mb, refresh_sec = metrics.get_table_stats(spark, catalog, schema_name, table_name, use_refresh=use_refresh)
        if use_refresh:
            _total_refresh_seconds += refresh_sec
            duration += refresh_sec
        metrics.record_table_load(_table_details, table_name, duration, row_count, size_mb, catalog, schema_name)
    return duration

def run_sql_multi_timed(rel_path, use_pipe_placeholder=False, incremental=False):
    """Run multi-statement SQL file and record duration + table stats.
    Incremental: stats = delta (rows/size added this run). No refresh so before/after counts are correct."""
    table_name = metrics.sql_file_to_table_name(rel_path)
    if table_name and incremental:
        count_before, size_before_mb, _ = metrics.get_table_stats(spark, catalog, schema_name, table_name, use_refresh=False)
    else:
        count_before, size_before_mb = 0, 0.0
    t0 = time.time()
    run_sql_multi(read_sql_file(rel_path), use_pipe_placeholder=use_pipe_placeholder)
    duration = time.time() - t0
    if table_name:
        count_after, size_after_mb, _ = metrics.get_table_stats(spark, catalog, schema_name, table_name, use_refresh=False)
        if incremental:
            row_count = max(0, count_after - count_before)
            size_mb = max(0.0, size_after_mb - size_before_mb)
        else:
            row_count, size_mb = count_after, size_after_mb
        metrics.record_table_load(_table_details, table_name, duration, row_count, size_mb, catalog, schema_name)
    return duration

# COMMAND ----------

job_start_time = None
job_end_time = None
_steps = []
_table_details = []
_total_refresh_seconds = 0.0

job_start_time = time.time()
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")
_steps.append({"step_name": "database_creation", "duration_seconds": time.time() - job_start_time, "rows_processed": 0})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch vs Incremental
# MAGIC If load_type is "incremental", run incremental load (Bronze → Silver → Gold) and exit. Otherwise continue with batch load.

# COMMAND ----------

is_incremental = (load_type == "incremental")
params = {"catalog": catalog, "schema_name": schema_name, "raw_data_path": raw_data_path, "sf": sf, "batch_id": batch_id, "xml_format": xml_format}

if is_incremental:
    print(f"Incremental load (batch_id={batch_id}): Bronze → Silver → Gold (per-table SQL)")
    bronze_incremental_files = [
        "sql/bronze/incremental/load_bronze_incremental_customer.sql",
        "sql/bronze/incremental/load_bronze_incremental_account.sql",
        "sql/bronze/incremental/load_bronze_incremental_trade.sql",
        "sql/bronze/incremental/load_bronze_incremental_daily_market.sql",
        "sql/bronze/incremental/load_bronze_incremental_cash_transaction.sql",
        "sql/bronze/incremental/load_bronze_incremental_holding_history.sql",
        "sql/bronze/incremental/load_bronze_incremental_watch_history.sql",
        "sql/bronze/incremental/load_bronze_incremental_prospect.sql",
    ]
    silver_incremental_files = [
        "sql/silver/incremental/transform_silver_incremental_customers.sql",
        "sql/silver/incremental/transform_silver_incremental_accounts.sql",
        "sql/silver/incremental/transform_silver_incremental_trades.sql",
        "sql/silver/incremental/transform_silver_incremental_daily_market.sql",
        "sql/silver/incremental/transform_silver_incremental_cash_transaction.sql",
        "sql/silver/incremental/transform_silver_incremental_holding_history.sql",
        "sql/silver/incremental/transform_silver_incremental_watch_history.sql",
        "sql/silver/incremental/transform_silver_incremental_prospect.sql",
    ]
    gold_incremental_files = [
        "sql/gold/incremental/load_gold_incremental_dim_customer.sql",
        "sql/gold/incremental/load_gold_incremental_dim_account.sql",
        "sql/gold/incremental/load_gold_incremental_dim_security.sql",
        "sql/gold/incremental/load_gold_incremental_dim_company.sql",
        "sql/gold/incremental/load_gold_incremental_financials.sql",
        "sql/gold/incremental/load_gold_incremental_fact_trade.sql",
        "sql/gold/incremental/load_gold_incremental_dim_messages.sql",
        "sql/gold/incremental/load_gold_incremental_fact_market_history.sql",
        "sql/gold/incremental/load_gold_incremental_fact_cash_balances.sql",
        "sql/gold/incremental/load_gold_incremental_fact_holdings.sql",
        "sql/gold/incremental/load_gold_incremental_fact_watches.sql",
        "sql/gold/incremental/load_gold_incremental_prospect.sql",
    ]
    _inc_bronze_start = time.time()
    _n_before = len(_table_details)
    for f in bronze_incremental_files:
        print(f"Bronze: {f}")
        run_sql_multi_timed(f, incremental=True)
    _inc_bronze_rows = sum(d["row_count"] for d in _table_details[_n_before:])
    _steps.append({"step_name": "bronze_etl", "duration_seconds": time.time() - _inc_bronze_start, "rows_processed": _inc_bronze_rows})

    _inc_silver_start = time.time()
    _n_before = len(_table_details)
    for f in silver_incremental_files:
        print(f"Silver: {f}")
        run_sql_multi_timed(f, incremental=True)
    _inc_silver_rows = sum(d["row_count"] for d in _table_details[_n_before:])
    _steps.append({"step_name": "silver_etl", "duration_seconds": time.time() - _inc_silver_start, "rows_processed": _inc_silver_rows})

    gold_optimize_files = [
        "sql/gold/optimize/optimize_gold_dim_company.sql",
        "sql/gold/optimize/optimize_gold_dim_customer.sql",
        "sql/gold/optimize/optimize_gold_dim_account.sql",
        "sql/gold/optimize/optimize_gold_dim_security.sql",
        "sql/gold/optimize/optimize_gold_dim_broker.sql",
        "sql/gold/optimize/optimize_gold_prospect.sql",
        "sql/gold/optimize/optimize_gold_fact_trade.sql",
        "sql/gold/optimize/optimize_gold_fact_holdings.sql",
        "sql/gold/optimize/optimize_gold_financials.sql",
    ]
    _opt_start = time.time()
    print("Gold: OPTIMIZE ZORDER (before incremental load)")
    for f in gold_optimize_files:
        print(f"  {f}")
        t0 = time.time()
        run_sql(read_sql_file(f))
        base = f.replace("\\", "/").split("/")[-1].replace(".sql", "")
        opt_table = base.replace("optimize_", "") if base.startswith("optimize_") else base
        metrics.record_table_load(_table_details, "optimize:" + opt_table, time.time() - t0, 0, 0.0, catalog, schema_name)
    _steps.append({"step_name": "gold_optimize", "duration_seconds": time.time() - _opt_start, "rows_processed": 0})

    print("Gold: CREATE TABLE IF NOT EXISTS gold_dim_messages")
    run_sql(read_sql_file("sql/gold/create_gold_dim_messages.sql"))
    dq_files_incr = [
        "sql/dq/dq_silver_date.sql", "sql/dq/dq_silver_status_type.sql", "sql/dq/dq_silver_trade_type.sql",
        "sql/dq/dq_silver_industry.sql",
        "sql/dq/dq_silver_companies.sql", "sql/dq/dq_silver_securities.sql", "sql/dq/dq_silver_financials.sql",
        "sql/dq/dq_silver_customers.sql", "sql/dq/dq_silver_accounts.sql", "sql/dq/dq_silver_trades.sql", "sql/dq/dq_silver_daily_market.sql",
        "sql/dq/dq_silver_cash_transaction.sql", "sql/dq/dq_silver_holding_history.sql", "sql/dq/dq_silver_watch_history.sql", "sql/dq/dq_silver_prospect.sql",
    ]
    print("DQ: Silver DQ rules (gold_dim_messages)")
    for rel in dq_files_incr:
        try:
            run_sql_multi(read_sql_file(rel))
        except Exception as e:
            print(f"DQ {rel} warning: {e}")

    _inc_gold_start = time.time()
    _n_before = len(_table_details)
    for f in gold_incremental_files:
        print(f"Gold: {f}")
        run_sql_multi_timed(f, incremental=True)
    _inc_gold_rows = sum(d["row_count"] for d in _table_details[_n_before:])
    _steps.append({"step_name": "gold_etl", "duration_seconds": time.time() - _inc_gold_start, "rows_processed": _inc_gold_rows})

    job_end_time = time.time()
    metrics.print_benchmark_report(spark, _steps, _table_details, job_start_time, job_end_time, catalog, schema_name, load_type, sf, total_refresh_seconds=_total_refresh_seconds)
    if metrics_output:
        metrics.save_metrics_output(spark, _steps, _table_details, job_start_time, time.time(), catalog, schema_name, load_type, sf, metrics_output, batch_id=batch_id, total_refresh_seconds=_total_refresh_seconds)
    dbutils.notebook.exit("Incremental load completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze (Batch 1)

# COMMAND ----------

_bronze_start = time.time()
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
    run_sql_timed(rel)

bronze_notebook_dir = (base_dir + "/sql/bronze/batch") if base_dir else "sql/bronze/batch"
for nb_name, table_short in [("load_bronze_customer_mgmt", "bronze_customer_mgmt"), ("load_bronze_finwire", "bronze_finwire")]:
    print(f"Bronze: {nb_name}")
    t0 = time.time()
    dbutils.notebook.run(bronze_notebook_dir + "/" + nb_name, timeout_seconds=600, arguments=params)
    rc, sz, refresh_sec = metrics.get_table_stats(spark, catalog, schema_name, table_short, use_refresh=True)
    _total_refresh_seconds += refresh_sec
    metrics.record_table_load(_table_details, table_short, time.time() - t0 + refresh_sec, rc, sz, catalog, schema_name)

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
    run_sql_timed(rel)
_bronze_rows = sum(d["row_count"] for d in _table_details)
_steps.append({"step_name": "bronze_etl", "duration_seconds": time.time() - _bronze_start, "rows_processed": _bronze_rows})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

_silver_start = time.time()
_n_before_silver = len(_table_details)
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
    run_sql_timed(rel, use_pipe_placeholder=True)

# COMMAND ----------

silver_notebook_dir = (base_dir + "/sql/silver/batch") if base_dir else "sql/silver/batch"
for nb_name, table_short in [("transform_silver_customers", "silver_customers"), ("transform_silver_accounts", "silver_accounts")]:
    print(f"Silver: {nb_name}")
    t0 = time.time()
    dbutils.notebook.run(silver_notebook_dir + "/" + nb_name, timeout_seconds=600, arguments=params)
    rc, sz, refresh_sec = metrics.get_table_stats(spark, catalog, schema_name, table_short, use_refresh=True)
    _total_refresh_seconds += refresh_sec
    metrics.record_table_load(_table_details, table_short, time.time() - t0 + refresh_sec, rc, sz, catalog, schema_name)
_silver_rows = sum(d["row_count"] for d in _table_details[_n_before_silver:])
_steps.append({"step_name": "silver_etl", "duration_seconds": time.time() - _silver_start, "rows_processed": _silver_rows})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality (Silver DQ → gold_dim_messages)
# MAGIC Ensures gold_dim_messages exists, then runs sql/dq/*.sql (one file per silver table).

# COMMAND ----------

print("DQ: CREATE TABLE IF NOT EXISTS gold_dim_messages")
run_sql(read_sql_file("sql/gold/create_gold_dim_messages.sql"))
dq_files = [
    "sql/dq/dq_silver_date.sql", "sql/dq/dq_silver_status_type.sql", "sql/dq/dq_silver_trade_type.sql",
    "sql/dq/dq_silver_industry.sql", "sql/dq/dq_silver_companies.sql", "sql/dq/dq_silver_securities.sql", "sql/dq/dq_silver_financials.sql",
    "sql/dq/dq_silver_customers.sql", "sql/dq/dq_silver_accounts.sql", "sql/dq/dq_silver_trades.sql", "sql/dq/dq_silver_daily_market.sql",
    "sql/dq/dq_silver_cash_transaction.sql", "sql/dq/dq_silver_holding_history.sql", "sql/dq/dq_silver_watch_history.sql", "sql/dq/dq_silver_prospect.sql",
]
_dq_start = time.time()
for rel in dq_files:
    try:
        run_sql_multi(read_sql_file(rel))
    except Exception as e:
        print(f"DQ {rel} warning: {e}")
_steps.append({"step_name": "silver_dq", "duration_seconds": time.time() - _dq_start, "rows_processed": 0})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

_gold_start = time.time()
_n_before_gold = len(_table_details)
gold_sql_files = [
    "sql/gold/load_gold_dim_date.sql",
    "sql/gold/load_gold_dim_time.sql",
    "sql/gold/load_gold_dim_status_type.sql",
    "sql/gold/load_gold_dim_trade_type.sql",
    "sql/gold/load_gold_dim_industry.sql",
    "sql/gold/load_gold_dim_customer.sql",
    "sql/gold/load_gold_dim_account.sql",
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
    run_sql_timed(rel)
_gold_rows = sum(d["row_count"] for d in _table_details[_n_before_gold:])
_steps.append({"step_name": "gold_etl", "duration_seconds": time.time() - _gold_start, "rows_processed": _gold_rows})

job_end_time = time.time()
metrics.print_benchmark_report(spark, _steps, _table_details, job_start_time, job_end_time, catalog, schema_name, load_type, sf, total_refresh_seconds=_total_refresh_seconds)
if metrics_output:
    metrics.save_metrics_output(spark, _steps, _table_details, job_start_time, job_end_time, catalog, schema_name, load_type, sf, metrics_output, batch_id=batch_id, total_refresh_seconds=_total_refresh_seconds)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
