# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Benchmark - Databricks
# MAGIC
# MAGIC Run TPC-DI ETL benchmark on Databricks with performance metrics.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - TPC-DI raw data generated (use `generate_tpcdi_data.py`)
# MAGIC - **Output path** = where raw data lives (DBFS or Unity Catalog Volume base):
# MAGIC   - DBFS: `dbfs:/mnt/tpcdi` → data at `dbfs:/mnt/tpcdi/sf=<scale_factor>/`
# MAGIC   - Volume: `/Volumes/<catalog>/<schema>/<volume>` → data at `.../volume/sf=<scale_factor>/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Widgets

# COMMAND ----------

try:
    dbutils.widgets.drop("load_type")
except Exception:
    pass
try:
    dbutils.widgets.drop("scale_factor")
except Exception:
    pass
try:
    dbutils.widgets.drop("tpcdi_raw_data_path")
except Exception:
    pass
try:
    dbutils.widgets.drop("target_schema")
except Exception:
    pass
try:
    dbutils.widgets.drop("target_catalog")
except Exception:
    pass
try:
    dbutils.widgets.drop("batch_id")
except Exception:
    pass
try:
    dbutils.widgets.drop("metrics_output")
except Exception:
    pass
try:
    dbutils.widgets.drop("log_detailed_stats")
except Exception:
    pass
try:
    dbutils.widgets.drop("use_udtf_customer_mgmt")
except Exception:
    pass
try:
    dbutils.widgets.drop("cloud")
except Exception:
    pass
try:
    dbutils.widgets.drop("customer_mgmt_xml_format")
except Exception:
    pass

dbutils.widgets.dropdown("load_type", "batch", ["batch", "incremental"], "Load Type")
dbutils.widgets.text("scale_factor", "10", "Scale Factor")
dbutils.widgets.text("tpcdi_raw_data_path", "dbfs:/mnt/tpcdi", "TPC-DI raw data path: dbfs:/... (DBFS), /Volumes/... (Volume)")
dbutils.widgets.text("target_schema", "dw", "Target Schema")
dbutils.widgets.text("target_catalog", "main", "Target Catalog (Unity Catalog; required)")
dbutils.widgets.text("batch_id", "", "Batch ID (for incremental only)")
dbutils.widgets.text("metrics_output", "dbfs:/mnt/tpcdi/metrics", "Metrics Output Path")
dbutils.widgets.dropdown("log_detailed_stats", "false", ["true", "false"], "Log detailed stats (per-table timing/records); false = only job start/end/total duration")
dbutils.widgets.dropdown("use_udtf_customer_mgmt", "false", ["auto", "true", "false"], "CustomerMgmt.xml: auto=false (spark-xml), true=UDTF, false=spark-xml")
dbutils.widgets.dropdown("customer_mgmt_xml_format", "xml", ["xml", "com.databricks.spark.xml"], "CustomerMgmt.xml format: xml or com.databricks.spark.xml")
dbutils.widgets.dropdown("cloud", "AWS", ["AWS", "Azure", "GCP"], "Cloud (for cost estimation: AWS, Azure, GCP)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Benchmark

# COMMAND ----------

import os
import sys
import logging
from pathlib import Path

# Configure logging to show DEBUG messages in notebook output
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    force=True  # Override any existing configuration
)

# Add project root to path (notebook in databricks/ → repo root = parent; else notebook dir)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_path = Path(notebook_path).parent
project_root = workspace_path.parent if (workspace_path.parent / "benchmark").is_dir() else workspace_path
sys.path.insert(0, str(project_root.resolve()))

from benchmark.config import BenchmarkConfig, Platform, LoadType
from benchmark.runner import run_benchmark

# Ensure benchmark modules also use DEBUG level
logging.getLogger('benchmark').setLevel(logging.DEBUG)
logging.getLogger('benchmark.runner').setLevel(logging.DEBUG)
logging.getLogger('benchmark.platforms').setLevel(logging.DEBUG)
logging.getLogger('benchmark.platforms.databricks').setLevel(logging.DEBUG)
# Suppress py4j (PySpark JVM bridge) DEBUG/INFO logs
logging.getLogger('py4j').setLevel(logging.WARNING)
logging.getLogger('py4j.clientserver').setLevel(logging.WARNING)

# Get parameters (from widgets or workflow parameters)
# Workflow parameters override widget defaults
load_type = dbutils.widgets.get("load_type")
scale_factor = int(dbutils.widgets.get("scale_factor"))
tpcdi_raw_data_path_raw = dbutils.widgets.get("tpcdi_raw_data_path").strip()
# Normalize: remove dbfs: prefix from Volume paths (load type inferred from path: dbfs=DBFS, /Volumes/=Volume)
tpcdi_raw_data_path = tpcdi_raw_data_path_raw
if tpcdi_raw_data_path.startswith("dbfs:/Volumes/"):
    tpcdi_raw_data_path = tpcdi_raw_data_path[5:]  # Remove "dbfs:" prefix
    print(f"WARNING: Removed 'dbfs:' prefix from Volume path: '{tpcdi_raw_data_path_raw}' -> '{tpcdi_raw_data_path}'")

target_schema = dbutils.widgets.get("target_schema").strip()
target_catalog = dbutils.widgets.get("target_catalog").strip()
if not target_catalog:
    raise ValueError("target_catalog is required for Databricks platform (Unity Catalog)")
batch_id_str = dbutils.widgets.get("batch_id").strip()
metrics_output = dbutils.widgets.get("metrics_output").strip()
log_detailed_stats = dbutils.widgets.get("log_detailed_stats") == "true"
use_udtf_customer_mgmt_str = dbutils.widgets.get("use_udtf_customer_mgmt").strip().lower()
use_udtf_customer_mgmt = {"auto": None, "true": True, "false": False}.get(use_udtf_customer_mgmt_str, None)
customer_mgmt_xml_format = dbutils.widgets.get("customer_mgmt_xml_format").strip() or "xml"
cloud = dbutils.widgets.get("cloud").strip() or "AWS"

# Parse batch_id for incremental loads
batch_id = int(batch_id_str) if batch_id_str and load_type == "incremental" else None

if load_type == "incremental" and batch_id is None:
    raise ValueError("batch_id is required for incremental loads")

config = BenchmarkConfig(
    platform=Platform.DATABRICKS,
    load_type=LoadType(load_type),
    scale_factor=scale_factor,
    raw_data_path=tpcdi_raw_data_path,
    target_schema=target_schema,
    target_catalog=target_catalog,
    output_path=tpcdi_raw_data_path,
    batch_id=batch_id,
    metrics_output_path=metrics_output,
    log_detailed_stats=log_detailed_stats,
    use_udtf_customer_mgmt=use_udtf_customer_mgmt,
    customer_mgmt_xml_format=customer_mgmt_xml_format,
    cloud=cloud,
)

result = run_benchmark(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

print("\n" + "="*80)
print("TPC-DI BENCHMARK RESULTS - DATABRICKS")
print("="*80)
print(f"Platform: {result['config']['platform']}")
if result['metrics'].get('databricks_compute_type'):
    print(f"Compute: {result['metrics']['databricks_compute_type']}")
print(f"Load Type: {result['config']['load_type']}")
print(f"Scale Factor: {result['config']['scale_factor']}")
if result['config']['batch_id']:
    print(f"Batch ID: {result['config']['batch_id']}")

# Cluster configuration
metrics_dict = result['metrics']
if metrics_dict.get('cluster_instance_type') or metrics_dict.get('cluster_worker_count') or metrics_dict.get('cluster_master_type'):
    print(f"\nCluster Configuration:")
    if metrics_dict.get('cluster_instance_type'):
        print(f"  Worker Node Type: {metrics_dict['cluster_instance_type']}")
    if metrics_dict.get('cluster_master_type'):
        print(f"  Driver Node Type: {metrics_dict['cluster_master_type']}")
    if metrics_dict.get('cluster_worker_count') is not None:
        print(f"  Number of Worker Nodes: {metrics_dict['cluster_worker_count']}")

# Table override information
if metrics_dict.get('table_override') is not None:
    print(f"\nTable Override: {metrics_dict['table_override']}")

print(f"\nTotal Duration: {result['metrics']['total_duration_seconds']:.2f} seconds")
if result['metrics']['summary']:
    summary = result['metrics']['summary']
    print(f"\nSummary:")
    print(f"  Total Steps: {summary['total_steps']}")
    print(f"  Completed Steps: {summary['completed_steps']}")
    print(f"  Failed Steps: {summary['failed_steps']}")
    print(f"  Total Rows Processed: {summary['total_rows_processed']:,}")
    print(f"  Total Data Size: {summary['total_bytes_processed'] / (1024*1024):.2f} MB")
    print(f"  Throughput: {summary['throughput_rows_per_second']:.2f} rows/sec")
    print(f"  Data Throughput: {summary['throughput_mb_per_second']:.2f} MB/sec")

dq_timings = result['metrics'].get('dq_table_timings')
if dq_timings:
    n_tables = len(dq_timings)
    print(f"\nDQ time per table ({n_tables} tables):")
    for t in dq_timings:
        print(f"  {t['table']}: {t['duration_seconds']:.2f}s")
    total_dq = sum(t['duration_seconds'] for t in dq_timings)
    print(f"  Total DQ: {total_dq:.2f}s")

# Cost (estimated; list-price approximation)
cb = result['metrics'].get('cost_breakdown')
total_cost = result['metrics'].get('total_cost_usd')
if cb is not None or total_cost is not None:
    print("\nCost (estimated):")
    if cb:
        if cb.get('compute_usd') is not None and (cb.get('compute_usd') or 0) > 0:
            print(f"  Compute: ${cb['compute_usd']:.2f}")
        if cb.get('software_usd') is not None:
            print(f"  Software: ${cb['software_usd']:.2f}")
    if total_cost is not None:
        print(f"  Total cost: ${total_cost:.2f}")
    dbu_cost = result['metrics'].get('dbu_cost_usd')
    if dbu_cost is not None:
        print(f"  DBU cost: ${dbu_cost:.2f}")

print("\nStep Details:")
for step in result['metrics']['steps']:
    status_icon = "✓" if step['status'] == "completed" else "✗" if step['status'] == "failed" else "○"
    print(f"  {status_icon} {step['step_name']}: {step['duration_seconds']:.2f}s", end="")
    if step['rows_processed']:
        print(f" ({step['rows_processed']:,} rows)", end="")
    if step['status'] == "failed":
        print(f" - ERROR: {step['error_message']}", end="")
    print()

if log_detailed_stats:
    try:
        from benchmark.etl.table_timing import get_summary as get_table_summary
        tsum = get_table_summary()
        details = tsum.get("table_details") or []
        if details:
            total_rows = tsum.get("total_records_loaded") or 0
            total_bytes = tsum.get("total_bytes_processed") or 0
            total_dur = tsum.get("total_duration_seconds") or 0
            total_mb = total_bytes / (1024 * 1024)
            rows_per_sec = total_rows / total_dur if total_dur > 0 else 0
            mb_per_sec = total_mb / total_dur if total_dur > 0 and total_bytes else 0
            print("\nTable-level stats:")
            print(f"  Tables loaded:      {len(details)}")
            print(f"  Total records:      {total_rows:,}")
            print(f"  Total data size:    {total_mb:.2f} MB")
            print(f"  Overall throughput: {rows_per_sec:,.1f} rows/s, {mb_per_sec:.2f} MB/s")
            print("  Per-table (duration, rows, size, throughput):")
            for d in details:
                dur = d.get("duration_seconds") or 0
                rows = d.get("row_count") or 0
                b = d.get("bytes_processed")
                row_s = rows / dur if dur > 0 else 0
                mb_s = (b / (1024 * 1024)) / dur if b and dur > 0 else None
                size_str = f", {b / (1024 * 1024):.2f} MB" if b else ""
                tp_str = f", {row_s:,.1f} rows/s" + (f", {mb_s:.2f} MB/s" if mb_s is not None else "")
                print(f"    - {d.get('table', '?')}: {dur:.2f}s, {rows:,} rows{size_str}{tp_str}")
    except Exception as e:
        print(f"\n  (Table-level stats unavailable: {e})")

print("="*80)

# COMMAND ----------
