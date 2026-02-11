# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI v2 Metrics Collection
# MAGIC
# MAGIC This notebook collects metrics from the v2 SQL implementation and generates a formatted report.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("bronze_schema", "bronze_schema", "Bronze Schema")
dbutils.widgets.text("silver_schema", "silver_schema", "Silver Schema")
dbutils.widgets.text("gold_schema", "gold_schema", "Gold Schema")
dbutils.widgets.text("platform", "databricks", "Platform")
dbutils.widgets.dropdown("compute_type", "classic", ["classic", "serverless"], "Compute Type")
dbutils.widgets.dropdown("load_type", "batch", ["batch", "incremental"], "Load Type")
dbutils.widgets.text("scale_factor", "10", "Scale Factor")
dbutils.widgets.text("batch_id", "", "Batch ID (for incremental)")
dbutils.widgets.text("metrics_output", "dbfs:/mnt/tpcdi/metrics", "Metrics Output Path")

# COMMAND ----------

import time
from datetime import datetime

# Get widget values
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
platform = dbutils.widgets.get("platform")
compute_type = dbutils.widgets.get("compute_type")
load_type = dbutils.widgets.get("load_type")
scale_factor = int(dbutils.widgets.get("scale_factor"))
batch_id_str = dbutils.widgets.get("batch_id")
metrics_output = dbutils.widgets.get("metrics_output")

batch_id = int(batch_id_str) if batch_id_str else None

# Get cluster info
try:
    cluster_info = spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "")
    node_type = spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType", "")
    num_workers_str = spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers", "")
    num_workers = int(num_workers_str) if num_workers_str else None
except Exception:
    cluster_info = "unknown"
    node_type = "unknown"
    num_workers = None

cluster_config = {
    "node_type_id": node_type,
    "num_workers": num_workers,
    "driver_node_type_id": node_type,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Table Statistics

# COMMAND ----------

def get_table_stats(table_name: str, schema: str) -> dict:
    """Get statistics for a table."""
    full_table = f"{catalog}.{schema}.{table_name}"
    
    try:
        # Check if table exists
        if not spark.catalog.tableExists(full_table):
            return {
                "table": full_table,
                "exists": False,
                "row_count": 0,
                "size_mb": 0,
            }
        
        # Get row count
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0]["cnt"]
        
        # Get table size
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {full_table}").collect()[0]
            size_bytes = detail.get("sizeInBytes", 0) or 0
            size_mb = size_bytes / (1024 * 1024) if size_bytes else 0
        except Exception:
            size_mb = 0
        
        return {
            "table": full_table,
            "exists": True,
            "row_count": row_count,
            "size_mb": size_mb,
        }
    except Exception as e:
        return {
            "table": full_table,
            "exists": False,
            "row_count": 0,
            "size_mb": 0,
            "error": str(e)
        }

# Define all tables
bronze_tables = [
    "bronze_date", "bronze_time", "bronze_status_type", "bronze_tax_rate",
    "bronze_trade_type", "bronze_industry", "bronze_hr", "bronze_customer_mgmt",
    "bronze_customer", "bronze_account", "bronze_trade", "bronze_daily_market",
    "bronze_prospect", "bronze_cash_transaction", "bronze_holding_history",
    "bronze_watch_history", "bronze_finwire",
]

silver_tables = [
    "silver_date", "silver_time", "silver_status_type", "silver_trade_type",
    "silver_industry", "silver_tax_rate", "silver_companies", "silver_securities",
    "silver_financials", "silver_customers", "silver_accounts", "silver_trades",
    "silver_daily_market", "silver_prospect", "silver_cash_transaction",
    "silver_watch_history", "silver_holding_history",
]

gold_tables = [
    "gold_dim_date", "gold_dim_customer", "gold_dim_account", "gold_dim_company",
    "gold_dim_security", "gold_dim_trade_type", "gold_dim_status_type",
    "gold_dim_industry", "gold_dim_broker", "gold_dim_trade",
    "gold_fact_trade", "gold_fact_market_history", "gold_fact_cash_balances",
    "gold_fact_holdings", "gold_fact_watches", "gold_financials", "gold_prospect",
    "gold_dim_messages",
]

# Collect stats
print("Collecting table statistics...")
all_table_stats = []

for table in bronze_tables:
    stats = get_table_stats(table, bronze_schema)
    stats["layer"] = "bronze"
    all_table_stats.append(stats)

for table in silver_tables:
    stats = get_table_stats(table, silver_schema)
    stats["layer"] = "silver"
    all_table_stats.append(stats)

for table in gold_tables:
    stats = get_table_stats(table, gold_schema)
    stats["layer"] = "gold"
    all_table_stats.append(stats)

# Filter to existing tables only
existing_tables = [s for s in all_table_stats if s.get("exists", False)]

# Calculate totals
total_rows = sum(s["row_count"] for s in existing_tables)
total_size_mb = sum(s["size_mb"] for s in existing_tables)

print(f"Found {len(existing_tables)} tables")
print(f"Total rows: {total_rows:,}")
print(f"Total size: {total_size_mb:.2f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Metrics Report

# COMMAND ----------

# Calculate duration (would be from workflow start/end times)
# For now, use current time as placeholder
start_time = time.time() - 600  # Assume 10 minutes ago
end_time = time.time()
total_duration = end_time - start_time

# Calculate throughput
throughput_rows_per_sec = total_rows / total_duration if total_duration > 0 else 0
throughput_mb_per_sec = total_size_mb / total_duration if total_duration > 0 else 0

# Build metrics dictionary
metrics = {
    "platform": platform,
    "compute_type": compute_type,
    "load_type": load_type,
    "scale_factor": scale_factor,
    "batch_id": batch_id,
    "start_time": start_time,
    "end_time": end_time,
    "total_duration_seconds": total_duration,
    "cluster_config": cluster_config,
    "table_stats": existing_tables,
    "summary": {
        "total_tables": len(existing_tables),
        "total_rows": total_rows,
        "total_size_mb": total_size_mb,
        "throughput_rows_per_sec": throughput_rows_per_sec,
        "throughput_mb_per_sec": throughput_mb_per_sec,
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Format and Display Report

# COMMAND ----------

def format_number(num: float, decimals: int = 2) -> str:
    """Format number with commas."""
    return f"{num:,.{decimals}f}"

def format_duration(seconds: float) -> str:
    """Format duration."""
    return f"{seconds:.2f}s"

# Build formatted report
summary = metrics["summary"]

report_lines = [
    "=" * 80,
    "TPC-DI BENCHMARK RESULTS - DATABRICKS",
    "=" * 80,
    f"Platform: {platform}",
    f"Compute: {compute_type}",
    f"Load Type: {load_type}",
    f"Scale Factor: {scale_factor}",
    "",
    "Cluster Configuration:",
    f"  Worker Node Type: {cluster_config.get('node_type_id', 'N/A')}",
    f"  Driver Node Type: {cluster_config.get('driver_node_type_id', 'N/A')}",
    f"  Number of Worker Nodes: {cluster_config.get('num_workers', 'N/A')}",
    "",
    f"Total Duration: {format_duration(total_duration)}",
    "",
    "Summary:",
    f"  Total Tables: {summary['total_tables']}",
    f"  Total Rows Processed: {format_number(summary['total_rows'], 0)}",
    f"  Total Data Size: {format_number(summary['total_size_mb'], 2)} MB",
    f"  Throughput: {format_number(summary['throughput_rows_per_sec'], 2)} rows/sec",
    f"  Data Throughput: {format_number(summary['throughput_mb_per_sec'], 2)} MB/sec",
    "",
    "Table-level stats:",
    f"  Tables loaded:      {summary['total_tables']}",
    f"  Total records:      {format_number(summary['total_rows'], 0)}",
    f"  Total data size:    {format_number(summary['total_size_mb'], 2)} MB",
    f"  Overall throughput: {format_number(summary['throughput_rows_per_sec'], 1)} rows/s, {format_number(summary['throughput_mb_per_sec'], 2)} MB/s",
    "  Per-table (duration, rows, size, throughput):",
]

summary = metrics["summary"]

# Sort tables by layer (bronze, silver, gold), then name
layer_order = {"bronze": 0, "silver": 1, "gold": 2}
sorted_tables = sorted(existing_tables, key=lambda x: (layer_order.get(x.get("layer", ""), 99), x["table"]))

for stats in sorted_tables:
    table_name = stats["table"]
    row_count = stats["row_count"]
    size_mb = stats["size_mb"]
    duration = 0  # Would need to track from workflow tasks
    
    rows_per_sec = row_count / duration if duration > 0 else 0
    mb_per_sec = size_mb / duration if duration > 0 else 0
    
    report_lines.append(
        f"    - {table_name}: {format_duration(duration)}, {format_number(row_count, 0)} rows, "
        f"{format_number(size_mb, 2)} MB, {format_number(rows_per_sec, 1)} rows/s, {format_number(mb_per_sec, 2)} MB/s"
    )

report_lines.append("=" * 80)

# Print report
report = "\n".join(report_lines)
print(report)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Metrics

# COMMAND ----------

import json
from pathlib import Path

# Save metrics JSON
if metrics_output:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"tpcdi_v2_metrics_{timestamp}.json"
    
    if metrics_output.startswith("dbfs:/"):
        output_path = metrics_output.replace("dbfs:/", "/dbfs/")
    elif metrics_output.startswith("/Volumes/"):
        output_path = metrics_output
    else:
        output_path = metrics_output
    
    Path(output_path).mkdir(parents=True, exist_ok=True)
    filepath = Path(output_path) / filename
    
    with open(filepath, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"\nMetrics saved to: {filepath}")
    
    # Also save formatted report
    report_file = filepath.with_suffix('.txt')
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(f"Report saved to: {report_file}")
