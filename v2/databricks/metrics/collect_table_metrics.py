# Databricks notebook source
# MAGIC %md
# MAGIC # Collect Table Metrics
# MAGIC
# MAGIC Collects metrics for a single table including row count, size, and timing.

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("table_name", "", "Table Name (e.g., bronze_date)")
dbutils.widgets.text("layer", "", "Layer (bronze, silver, gold)")
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
table_name = dbutils.widgets.get("table_name")
layer = dbutils.widgets.get("layer")
sf = int(dbutils.widgets.get("sf"))
batch_id = int(dbutils.widgets.get("batch_id"))
metrics_output = dbutils.widgets.get("metrics_output")

if not table_name:
    raise ValueError("table_name parameter is required")

full_table = f"{catalog}.{schema_name}.{table_name}"

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
# MAGIC ## Collect Table Statistics

# COMMAND ----------

start_time = time.time()

try:
    if not spark.catalog.tableExists(full_table):
        print(f"⚠️  Table {full_table} does not exist")
        metrics = {
            "table": full_table,
            "layer": layer,
            "exists": False,
            "row_count": 0,
            "size_mb": 0,
            "error": "Table does not exist"
        }
    else:
        # Get row count
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table}").collect()[0]["cnt"]
        
        # Get table size
        try:
            detail = spark.sql(f"DESCRIBE DETAIL {full_table}").collect()[0]
            size_bytes = detail.get("sizeInBytes", 0) or 0
            size_mb = size_bytes / (1024 * 1024) if size_bytes else 0
        except Exception as e:
            size_mb = 0
            print(f"⚠️  Could not get size for {full_table}: {e}")
        
        end_time = time.time()
        duration = end_time - start_time
        
        metrics = {
            "table": full_table,
            "layer": layer,
            "exists": True,
            "row_count": row_count,
            "size_mb": size_mb,
            "collection_time": datetime.now().isoformat(),
            "duration_seconds": duration,
            "cluster_config": {
                "node_type_id": node_type,
                "num_workers": num_workers,
            },
            "catalog": catalog,
            "schema_name": schema_name,
            "sf": sf,
            "batch_id": batch_id,
        }
        
        print(f"✓ {table_name}: {row_count:,} rows, {size_mb:.2f} MB, {duration:.2f}s")
        
except Exception as e:
    end_time = time.time()
    duration = end_time - start_time
    print(f"✗ {table_name}: Error - {e}")
    metrics = {
        "table": full_table,
        "layer": layer,
        "exists": False,
        "row_count": 0,
        "size_mb": 0,
        "error": str(e),
        "duration_seconds": duration,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Metrics

# COMMAND ----------

# Save metrics
if metrics_output:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{table_name}_metrics_sf{sf}_batch{batch_id}_{timestamp}.json"
    
    if metrics_output.startswith("dbfs:/"):
        output_path = metrics_output.replace("dbfs:/", "/dbfs/")
    else:
        output_path = metrics_output
    
    Path(output_path).mkdir(parents=True, exist_ok=True)
    filepath = Path(output_path) / filename
    
    with open(filepath, 'w') as f:
        json.dump(metrics, f, indent=2)
    
    print(f"\n✅ Metrics saved to: {filepath}")
