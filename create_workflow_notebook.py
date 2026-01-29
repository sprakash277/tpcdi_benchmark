# Databricks notebook source
# MAGIC %md
# MAGIC # Create TPC-DI Benchmark Workflow
# MAGIC
# MAGIC This notebook creates a Databricks workflow/job that:
# MAGIC 1. Generates TPC-DI raw data
# MAGIC 2. Runs the benchmark ETL
# MAGIC
# MAGIC All parameters are configurable via workflow parameters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for workflow creation
dbutils.widgets.text("job_name", "TPC-DI-Benchmark", "Job Name")
dbutils.widgets.text("data_gen_notebook", "generate_tpcdi_data_notebook", "Data Generation Notebook Path")
dbutils.widgets.text("benchmark_notebook", "benchmark_databricks_notebook", "Benchmark Notebook Path")
dbutils.widgets.dropdown(
    "spark_version",
    "14.3.x-scala2.12",
    [
        "13.3.x-scala2.12",
        "13.3.x-photon-scala2.12",
        "14.3.x-scala2.12",
        "14.3.x-photon-scala2.12",
        "15.4.x-scala2.12",
        "15.4.x-photon-scala2.12",
        "16.4.x-scala2.12",
        "16.4.x-photon-scala2.12",
    ],
    "Cluster Spark Version (DBR)"
)
dbutils.widgets.text("node_type_id", "i3.xlarge", "Worker Node Type")
dbutils.widgets.text("driver_node_type_id", "i3.xlarge", "Driver Node Type")
dbutils.widgets.text("num_workers", "2", "Number of Workers")
dbutils.widgets.text("existing_cluster_id", "", "Existing Cluster ID (optional)")

# COMMAND ----------

import json
from pathlib import Path

job_name = dbutils.widgets.get("job_name")
data_gen_notebook = dbutils.widgets.get("data_gen_notebook")
benchmark_notebook = dbutils.widgets.get("benchmark_notebook")
spark_version = dbutils.widgets.get("spark_version")
node_type_id = dbutils.widgets.get("node_type_id")
driver_node_type_id = dbutils.widgets.get("driver_node_type_id")
num_workers = int(dbutils.widgets.get("num_workers"))
existing_cluster_id = dbutils.widgets.get("existing_cluster_id").strip()

# Get workspace path from current notebook path (parent directory)
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    workspace_path = str(Path(notebook_path).parent)
except Exception:
    workspace_path = "/Workspace/Repos"

# Construct full notebook paths
if not data_gen_notebook.startswith("/"):
    data_gen_notebook = f"{workspace_path}/{data_gen_notebook}"
if not benchmark_notebook.startswith("/"):
    benchmark_notebook = f"{workspace_path}/{benchmark_notebook}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Workflow Definition

# COMMAND ----------

# Build cluster config
cluster_config = {
    "spark_version": spark_version,
    "node_type_id": node_type_id,
    "num_workers": num_workers,
    "driver_node_type_id": driver_node_type_id,
}

# Create workflow definition
workflow = {
    "name": job_name,
    "email_notifications": {
        "on_start": [],
        "on_success": [],
        "on_failure": [],
        "no_alert_for_skipped_runs": False
    },
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "format": "MULTI_TASK",
    "tasks": [
        {
            "task_key": "01_data_generation",
            "description": "Generate TPC-DI raw data",
            "notebook_task": {
                "notebook_path": data_gen_notebook,
                "base_parameters": {
                    "scale_factor": "10",
                    "tpcdi_raw_data_path": "dbfs:/mnt/tpcdi",
                    "upload_threads": "8"
                }
            },
            "timeout_seconds": 0,
            "email_notifications": {},
            "retry_on_timeout": False,
            "max_retries": 0,
            "min_retry_interval_millis": 0,
            "max_retry_interval_millis": 0,
        },
        {
            "task_key": "02_benchmark_execution",
            "description": "Run TPC-DI benchmark ETL",
            "depends_on": [
                {
                    "task_key": "01_data_generation"
                }
            ],
            "notebook_task": {
                "notebook_path": benchmark_notebook,
                "base_parameters": {
                    "load_type": "batch",
                    "scale_factor": "10",
                    "tpcdi_raw_data_path": "dbfs:/mnt/tpcdi",
                    "target_database": "tpcdi_warehouse",
                    "target_schema": "dw",
                    "target_catalog": "",
                    "batch_id": "",
                    "metrics_output": "dbfs:/mnt/tpcdi/metrics",
                    "log_detailed_stats": "false"
                }
            },
            "timeout_seconds": 0,
            "email_notifications": {},
            "retry_on_timeout": False,
            "max_retries": 0,
            "min_retry_interval_millis": 0,
            "max_retry_interval_millis": 0,
        }
    ],
    "parameters": [
        {
            "name": "scale_factor",
            "default": "10",
            "description": "TPC-DI scale factor (e.g., 10, 100, 1000)"
        },
        {
            "name": "tpcdi_raw_data_path",
            "default": "dbfs:/mnt/tpcdi",
            "description": "TPC-DI raw data path (used by both 01_data_generation and 02_benchmark_execution); dbfs:/..., /Volumes/..., or gs://..."
        },
        {
            "name": "load_type",
            "default": "batch",
            "description": "Load type: batch or incremental"
        },
        {
            "name": "target_database",
            "default": "tpcdi_warehouse",
            "description": "Target database name"
        },
        {
            "name": "target_schema",
            "default": "dw",
            "description": "Target schema name"
        },
        {
            "name": "target_catalog",
            "default": "",
            "description": "Unity Catalog name (optional); when set, create catalog + schema"
        },
        {
            "name": "batch_id",
            "default": "",
            "description": "Batch ID for incremental loads (leave empty for batch)"
        },
        {
            "name": "metrics_output",
            "default": "dbfs:/mnt/tpcdi/metrics",
            "description": "Path to save metrics JSON files"
        },
        {
            "name": "log_detailed_stats",
            "default": "false",
            "description": "Log per-table timing and records; false = only job start/end/total duration"
        },
        {
            "name": "upload_threads",
            "default": "8",
            "description": "Number of parallel threads for DBFS uploads"
        }
    ],
    "tags": {
        "purpose": "tpcdi_benchmark",
        "component": "data_integration"
    }
}

# Add cluster config to tasks
if existing_cluster_id:
    # Use existing cluster
    for task in workflow["tasks"]:
        task["existing_cluster_id"] = existing_cluster_id
else:
    # Create new clusters
    for task in workflow["tasks"]:
        task["new_cluster"] = cluster_config.copy()

print("Workflow definition created:")
print(json.dumps(workflow, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Job via Databricks API

# COMMAND ----------

import requests

# Get Databricks host and token
databricks_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
databricks_token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# Create job
url = f"{databricks_host}/api/2.1/jobs/create"
headers = {
    "Authorization": f"Bearer {databricks_token}",
    "Content-Type": "application/json"
}

response = requests.post(url, headers=headers, json=workflow)

if response.status_code == 200:
    result = response.json()
    job_id = result.get("job_id")
    print(f"✓ Workflow created successfully!")
    print(f"  Job ID: {job_id}")
    print(f"  Job Name: {workflow['name']}")
    print(f"\nView job at: {databricks_host.replace('/api', '')}/#job/{job_id}")
    
    # Save job ID for reference
    dbutils.notebook.exit(json.dumps({"job_id": job_id, "job_name": workflow['name']}))
else:
    print(f"✗ Failed to create workflow")
    print(f"  Status Code: {response.status_code}")
    print(f"  Response: {response.text}")
    raise Exception(f"Failed to create workflow: {response.text}")

# COMMAND ----------
