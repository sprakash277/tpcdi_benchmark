# Databricks notebook source
# MAGIC %md
# MAGIC # Collect Silver Layer Metrics
# MAGIC
# MAGIC Collects metrics for all Silver tables including row counts, sizes, and timing.

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("sf", "10", "Scale Factor")
dbutils.widgets.text("batch_id", "1", "Batch ID")
dbutils.widgets.text("metrics_output", "dbfs:/mnt/tpcdi/metrics", "Metrics Output Path")

# COMMAND ----------

import time
import json
from datetime import datetime
from pathlib import Path

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
sf = int(dbutils.widgets.get("sf"))
batch_id = int(dbutils.widgets.get("batch_id"))
metrics_output = dbutils.widgets.get("metrics_output")

# Get cluster info
try:
    node_type = spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType", "unknown")
    num_workers_str = spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers", "")
    num_workers = int(num_workers_str) if num_workers_str else None
except Exception:
    node_type = "unknown"
    num_workers = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Silver Table Statistics

# COMMAND ----------

silver_tables = [
    "silver_date", "silver_time", "silver_status_type", "silver_trade_type",
    "silver_industry", "silver_tax_rate", "silver_companies", "silver_securities",
    "silver_financials", "silver_customers", "silver_accounts", "silver_trades",
    "silver_daily_market", "silver_prospect", "silver_cash_transaction",
    "silver_watch_history", "silver_holding_history",
]

table_stats = []
start_time = time.time()

for table in silver_tables:
    full_table = f"{catalog}.{schema_name}.{table}"
    
    try:
        if not spark.catalog.tableExists(full_table):
            continue
        
        # Get row count
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0]["cnt"]
        
        # Get table size
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {full_table}").collect()[0]
            size_bytes = detail.get("sizeInBytes", 0) or 0
            size_mb = size_bytes / (1024 * 1024) if size_bytes else 0
        except Exception:
            size_mb = 0
        
        table_stats.append({
            "table": full_table,
            "layer": "silver",
            "row_count": row_count,
            "size_mb": size_mb,
        })
        
        print(f"✓ {table}: {row_count:,} rows, {size_mb:.2f} MB")
    except Exception as e:
        print(f"✗ {table}: Error - {e}")

end_time = time.time()
duration = end_time - start_time

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Metrics

# COMMAND ----------

total_rows = sum(s["row_count"] for s in table_stats)
total_size_mb = sum(s["size_mb"] for s in table_stats)

metrics = {
    "layer": "silver",
    "catalog": catalog,
    "schema_name": schema_name,
    "sf": sf,
    "batch_id": batch_id,
    "collection_time": datetime.now().isoformat(),
    "duration_seconds": duration,
    "cluster_config": {
        "node_type_id": node_type,
        "num_workers": num_workers,
    },
    "table_stats": table_stats,
    "summary": {
        "total_tables": len(table_stats),
        "total_rows": total_rows,
        "total_size_mb": total_size_mb,
    }
}

# Save metrics
if metrics_output:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"silver_metrics_sf{sf}_batch{batch_id}_{timestamp}.json"
    
    if metrics_output.startswith("dbfs:/"):
        output_path = metrics_output.replace("dbfs:/", "/dbfs/")
    else:
        output_path = metrics_output
    
    Path(output_path).mkdir(parents=True, exist_ok=True)
    filepath = Path(output_path) / filename
    
    with open(filepath, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"\n✅ Silver metrics saved to: {filepath}")
    print(f"   Tables: {len(table_stats)}")
    print(f"   Total rows: {total_rows:,}")
    print(f"   Total size: {total_size_mb:.2f} MB")
