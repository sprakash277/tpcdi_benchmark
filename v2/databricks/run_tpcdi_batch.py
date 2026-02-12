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

# COMMAND ----------

import os
import time

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = dbutils.widgets.get("batch_id")
load_type = dbutils.widgets.get("load_type") or "batch"
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

# --- Metrics (V1-style report)
def _sql_file_to_table_name(rel_path):
    """Map SQL file path to short table name (e.g. sql/bronze/load_bronze_date.sql -> bronze_date)."""
    base = rel_path.replace("\\", "/").split("/")[-1].replace(".sql", "")
    if base.startswith("load_bronze_"): return "bronze_" + base[len("load_bronze_"):]
    if base.startswith("load_gold_incremental_"): return "gold_" + base[len("load_gold_incremental_"):]
    if base.startswith("load_gold_"): return "gold_" + base[len("load_gold_"):]
    if base.startswith("transform_silver_incremental_"): return "silver_" + base[len("transform_silver_incremental_"):]
    if base.startswith("transform_silver_"): return "silver_" + base[len("transform_silver_"):]
    if base.startswith("load_bronze_incremental_"): return "bronze_" + base[len("load_bronze_incremental_"):]
    return None

def _get_table_stats(table_short_name):
    """Return (row_count, size_mb) for catalog.schema.table_short_name. Returns (0, 0) if table missing or error."""
    full = f"{catalog}.{schema_name}.{table_short_name}"
    try:
        if not spark.catalog.tableExists(full):
            return 0, 0.0
        row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full}").collect()[0]["cnt"]
        detail = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]
        size_bytes = detail.get("sizeInBytes") or 0
        size_mb = size_bytes / (1024 * 1024) if size_bytes else 0.0
        return row_count, size_mb
    except Exception:
        return 0, 0.0

def _record_table_load(table_key, duration_seconds, row_count, size_mb):
    """Append one entry to table_details. table_key can be 'gold_dim_customer' or 'optimize:gold_dim_company'."""
    full = f"{catalog}.{schema_name}.{table_key}" if ":" not in table_key else table_key
    bytes_processed = int(size_mb * 1024 * 1024) if size_mb else 0
    _table_details.append({
        "table": full,
        "duration_seconds": duration_seconds,
        "row_count": row_count,
        "bytes_processed": bytes_processed,
    })

def run_sql_timed(rel_path, use_pipe_placeholder=False):
    """Run single-statement SQL file and record duration + table stats."""
    table_name = _sql_file_to_table_name(rel_path)
    t0 = time.time()
    run_sql(read_sql_file(rel_path), use_pipe_placeholder=use_pipe_placeholder)
    duration = time.time() - t0
    if table_name:
        row_count, size_mb = _get_table_stats(table_name)
        _record_table_load(table_name, duration, row_count, size_mb)
    return duration

def run_sql_multi_timed(rel_path, use_pipe_placeholder=False):
    """Run multi-statement SQL file and record duration + table stats (from last/primary table in file)."""
    table_name = _sql_file_to_table_name(rel_path)
    t0 = time.time()
    run_sql_multi(read_sql_file(rel_path), use_pipe_placeholder=use_pipe_placeholder)
    duration = time.time() - t0
    if table_name:
        row_count, size_mb = _get_table_stats(table_name)
        _record_table_load(table_name, duration, row_count, size_mb)
    return duration

def _print_benchmark_report():
    """Print TPC-DI benchmark results in V1 format (steps, table-level stats, optional cost)."""
    total_duration = (job_end_time - job_start_time) if job_end_time and job_start_time else 0
    total_rows = sum(d["row_count"] for d in _table_details)
    total_bytes = sum(d.get("bytes_processed") or 0 for d in _table_details)
    total_mb = total_bytes / (1024 * 1024) if total_bytes else 0
    rows_per_sec = total_rows / total_duration if total_duration > 0 else 0
    mb_per_sec = total_mb / total_duration if total_duration > 0 else 0
    completed = sum(1 for s in _steps if s.get("duration_seconds", 0) >= 0)
    failed = 0

    try:
        worker_type = spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType", "N/A")
        driver_type = spark.conf.get("spark.databricks.clusterUsageTags.clusterDriverNodeType", worker_type)
        num_workers_str = spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers", "")
        num_workers = int(num_workers_str) if num_workers_str else "N/A"
    except Exception:
        worker_type = driver_type = "N/A"
        num_workers = "N/A"

    sep = "=" * 80
    lines = [
        "",
        sep,
        "TPC-DI BENCHMARK RESULTS - DATABRICKS",
        sep,
        "Platform: databricks",
        "Compute: classic",
        f"Load Type: {load_type}",
        f"Scale Factor: {sf}",
        "",
        "Cluster Configuration:",
        f"  Worker Node Type: {worker_type}",
        f"  Driver Node Type: {driver_type}",
        f"  Number of Worker Nodes: {num_workers}",
        "",
        "Table Override: True",
        "",
        f"Total Duration: {total_duration:.2f} seconds",
        "",
        "Summary:",
        f"  Total Steps: {len(_steps)}",
        f"  Completed Steps: {completed}",
        f"  Failed Steps: {failed}",
        f"  Total Rows Processed: {total_rows:,}",
        f"  Total Data Size: {total_mb:.2f} MB",
        f"  Throughput: {rows_per_sec:.2f} rows/sec",
        f"  Data Throughput: {mb_per_sec:.2f} MB/sec",
        "",
    ]

    lines.append("DQ time per table (N/A for v2 SQL pipeline):")
    lines.append("  (v2 does not run separate DQ step)")
    lines.append("")

    try:
        from benchmark.cost import estimate_databricks_cost
        cost = estimate_databricks_cost(
            total_duration_seconds=total_duration,
            cluster_worker_count=num_workers if isinstance(num_workers, int) else 4,
            cluster_instance_type=worker_type if worker_type != "N/A" else None,
            cluster_master_type=driver_type if driver_type != "N/A" else None,
            databricks_compute_type="classic",
            cloud="GCP",
        )
        if cost:
            cb = cost.get("cost_breakdown") or {}
            total_cost = cost.get("total_cost_usd")
            dbu_cost = cost.get("dbu_cost_usd")
            lines.append("Cost (estimated):")
            if cb.get("compute_usd") is not None:
                lines.append(f"  Compute: ${cb['compute_usd']:.2f}")
            if cb.get("software_usd") is not None:
                lines.append(f"  Software: ${cb['software_usd']:.2f}")
            if total_cost is not None:
                lines.append(f"  Total cost: ${total_cost:.2f}")
            if dbu_cost is not None:
                lines.append(f"  DBU cost: ${dbu_cost:.2f}")
            lines.append("")
    except Exception:
        lines.append("Cost (estimated): N/A (benchmark.cost not available)")
        lines.append("")

    lines.append("Step Details:")
    for s in _steps:
        name = s.get("step_name", "?")
        dur = s.get("duration_seconds", 0)
        rows = s.get("rows_processed", 0)
        icon = "✓"
        lines.append(f"  {icon} {name}: {dur:.2f}s" + (f" ({rows:,} rows)" if rows else ""))
    lines.append("")

    lines.append("Table-level stats:")
    lines.append(f"  Tables loaded:      {len(_table_details)}")
    lines.append(f"  Total records:      {total_rows:,}")
    lines.append(f"  Total data size:    {total_mb:.2f} MB")
    lines.append(f"  Overall throughput: {rows_per_sec:,.1f} rows/s, {mb_per_sec:.2f} MB/s")
    lines.append("  Per-table (duration, rows, size, throughput):")
    for d in _table_details:
        tbl = d.get("table", "?")
        dur = d.get("duration_seconds") or 0
        rows = d.get("row_count") or 0
        b = d.get("bytes_processed") or 0
        size_mb = b / (1024 * 1024) if b else 0
        row_s = rows / dur if dur > 0 else 0
        mb_s = size_mb / dur if dur > 0 else 0
        lines.append(f"    - {tbl}: {dur:.2f}s, {rows:,} rows, {size_mb:.2f} MB, {row_s:,.1f} rows/s, {mb_s:.2f} MB/s")
    lines.append(sep)
    print("\n".join(lines))

# COMMAND ----------

job_start_time = None
job_end_time = None
_steps = []
_table_details = []

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
        run_sql_multi_timed(f)
    _inc_bronze_rows = sum(d["row_count"] for d in _table_details[_n_before:])
    _steps.append({"step_name": "bronze_etl", "duration_seconds": time.time() - _inc_bronze_start, "rows_processed": _inc_bronze_rows})

    _inc_silver_start = time.time()
    _n_before = len(_table_details)
    for f in silver_incremental_files:
        print(f"Silver: {f}")
        run_sql_multi_timed(f)
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
        _record_table_load("optimize:" + opt_table, time.time() - t0, 0, 0.0)
    _steps.append({"step_name": "gold_optimize", "duration_seconds": time.time() - _opt_start, "rows_processed": 0})

    print("Gold: CREATE TABLE IF NOT EXISTS gold_dim_messages")
    run_sql(read_sql_file("sql/gold/create_gold_dim_messages.sql"))

    _inc_gold_start = time.time()
    _n_before = len(_table_details)
    for f in gold_incremental_files:
        print(f"Gold: {f}")
        run_sql_multi_timed(f)
    _inc_gold_rows = sum(d["row_count"] for d in _table_details[_n_before:])
    _steps.append({"step_name": "gold_etl", "duration_seconds": time.time() - _inc_gold_start, "rows_processed": _inc_gold_rows})

    job_end_time = time.time()
    _print_benchmark_report()
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

bronze_notebook_dir = (base_dir + "/bronze/batch") if base_dir else "bronze/batch"
for nb_name, table_short in [("load_bronze_customer_mgmt", "bronze_customer_mgmt"), ("load_bronze_finwire", "bronze_finwire")]:
    print(f"Bronze: {nb_name}")
    t0 = time.time()
    dbutils.notebook.run(bronze_notebook_dir + "/" + nb_name, timeout_seconds=600, arguments=params)
    rc, sz = _get_table_stats(table_short)
    _record_table_load(table_short, time.time() - t0, rc, sz)

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

silver_notebook_dir = base_dir + "/silver/batch" if base_dir else "silver/batch"
for nb_name, table_short in [("transform_silver_customers", "silver_customers"), ("transform_silver_accounts", "silver_accounts")]:
    print(f"Silver: {nb_name}")
    t0 = time.time()
    dbutils.notebook.run(silver_notebook_dir + "/" + nb_name, timeout_seconds=600, arguments=params)
    rc, sz = _get_table_stats(table_short)
    _record_table_load(table_short, time.time() - t0, rc, sz)
_silver_rows = sum(d["row_count"] for d in _table_details[_n_before_silver:])
_steps.append({"step_name": "silver_etl", "duration_seconds": time.time() - _silver_start, "rows_processed": _silver_rows})

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
_print_benchmark_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done
