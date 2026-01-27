# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Benchmark - Databricks
# MAGIC
# MAGIC Run TPC-DI ETL benchmark on Databricks with performance metrics.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - TPC-DI raw data generated and stored in DBFS (use `generate_tpcdi_data.py`)
# MAGIC - Data should be at: `dbfs:/mnt/tpcdi/sf=<scale_factor>/`

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
    dbutils.widgets.drop("raw_data_path")
except Exception:
    pass
try:
    dbutils.widgets.drop("target_database")
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

dbutils.widgets.dropdown("load_type", "batch", ["batch", "incremental"], "Load Type")
dbutils.widgets.text("scale_factor", "10", "Scale Factor")
dbutils.widgets.text("raw_data_path", "dbfs:/mnt/tpcdi", "Raw Data Base Path (DBFS)")
dbutils.widgets.text("target_database", "tpcdi_warehouse", "Target Database")
dbutils.widgets.text("target_schema", "dw", "Target Schema")
dbutils.widgets.text("target_catalog", "", "Target Catalog (Unity Catalog; optional)")
dbutils.widgets.text("batch_id", "", "Batch ID (for incremental only)")
dbutils.widgets.text("metrics_output", "dbfs:/mnt/tpcdi/metrics", "Metrics Output Path")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Benchmark

# COMMAND ----------

import os
import sys
from pathlib import Path

# Add benchmark module to path: derive from notebook path (parent of current notebook)
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workspace_path = str(Path(notebook_path).parent)
sys.path.insert(0, str(Path(workspace_path).resolve()))

from benchmark.config import BenchmarkConfig, Platform, LoadType
from benchmark.runner import run_benchmark

# Get parameters (from widgets or workflow parameters)
# Workflow parameters override widget defaults
load_type = dbutils.widgets.get("load_type")
scale_factor = int(dbutils.widgets.get("scale_factor"))
raw_data_base = dbutils.widgets.get("raw_data_path").strip()
target_database = dbutils.widgets.get("target_database").strip()
target_schema = dbutils.widgets.get("target_schema").strip()
target_catalog = dbutils.widgets.get("target_catalog").strip() or None
batch_id_str = dbutils.widgets.get("batch_id").strip()
metrics_output = dbutils.widgets.get("metrics_output").strip()

print(f"Benchmark Parameters:")
print(f"  Load Type: {load_type}")
print(f"  Scale Factor: {scale_factor}")
print(f"  Raw Data Path: {raw_data_base}")
print(f"  Target Database: {target_database}")
print(f"  Target Schema: {target_schema}")
print(f"  Target Catalog: {target_catalog or 'N/A (Hive metastore)'}")
print(f"  Batch ID: {batch_id_str if batch_id_str else 'N/A (batch load)'}")
print(f"  Metrics Output: {metrics_output}")

# Construct full raw data path
raw_data_path = f"{raw_data_base.rstrip('/')}/sf={scale_factor}"

# Parse batch_id for incremental loads
batch_id = int(batch_id_str) if batch_id_str and load_type == "incremental" else None

if load_type == "incremental" and batch_id is None:
    raise ValueError("batch_id is required for incremental loads")

# Create configuration
config = BenchmarkConfig(
    platform=Platform.DATABRICKS,
    load_type=LoadType(load_type),
    scale_factor=scale_factor,
    raw_data_path=raw_data_path,
    target_database=target_database,
    target_schema=target_schema,
    target_catalog=target_catalog,
    batch_id=batch_id,
    metrics_output_path=metrics_output,
)

# Run benchmark
result = run_benchmark(config)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results Summary

# COMMAND ----------

print("\n" + "="*80)
print("TPC-DI BENCHMARK RESULTS - DATABRICKS")
print("="*80)
print(f"Platform: {result['config']['platform']}")
print(f"Load Type: {result['config']['load_type']}")
print(f"Scale Factor: {result['config']['scale_factor']}")
if result['config']['batch_id']:
    print(f"Batch ID: {result['config']['batch_id']}")
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

print("\nStep Details:")
for step in result['metrics']['steps']:
    status_icon = "✓" if step['status'] == "completed" else "✗" if step['status'] == "failed" else "○"
    print(f"  {status_icon} {step['step_name']}: {step['duration_seconds']:.2f}s", end="")
    if step['rows_processed']:
        print(f" ({step['rows_processed']:,} rows)", end="")
    if step['status'] == "failed":
        print(f" - ERROR: {step['error_message']}", end="")
    print()

print("="*80)

# COMMAND ----------
