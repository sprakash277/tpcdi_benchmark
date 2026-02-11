# Databricks notebook source
# MAGIC %md
# MAGIC # Create Workflow for run_tpcdi_batch
# MAGIC
# MAGIC Run this notebook to create a Databricks job that runs **run_tpcdi_batch** (Bronze → Silver → Gold).  
# MAGIC Set the widgets below, then run all. The job will appear under Workflows.

# COMMAND ----------

# Job and notebook
dbutils.widgets.text("job_name", "TPC-DI v2 Batch", "Job name")
dbutils.widgets.text("run_notebook_path", "v2/databricks/run_tpcdi_batch", "Notebook path (run_tpcdi_batch)")
dbutils.widgets.text("workspace_path", "", "Workspace path prefix (e.g. /Workspace/Repos/org/repo); empty = use path as-is)")

# Default parameters for the pipeline (used as job default; overridable at run time)
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema name")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw data path")
dbutils.widgets.text("sf", "10", "Scale factor")
dbutils.widgets.text("batch_id", "1", "Batch ID")
dbutils.widgets.text("xml_format", "com.databricks.spark.xml", "XML format")
dbutils.widgets.text("sql_base_path", "", "SQL base path (optional)")

# Cluster: new job cluster or existing
dbutils.widgets.text("spark_version", "14.3.x-scala2.12", "Spark version")
dbutils.widgets.text("node_type_id", "i3.xlarge", "Worker node type")
dbutils.widgets.text("num_workers", "2", "Number of workers")
dbutils.widgets.text("driver_node_type_id", "i3.xlarge", "Driver node type")
dbutils.widgets.text("existing_cluster_id", "", "Existing cluster ID (optional; if set, job uses this instead of job cluster)")

# API (token from widget or secret)
dbutils.widgets.text("databricks_host", "", "Workspace URL (e.g. https://xxx.cloud.databricks.com; empty = try spark.conf)")
dbutils.widgets.text("databricks_token", "", "Personal access token (or leave empty and set token_secret_scope/key)")
dbutils.widgets.text("token_secret_scope", "", "Secret scope for token (optional)")
dbutils.widgets.text("token_secret_key", "", "Secret key for token (optional)")

# COMMAND ----------

job_name = dbutils.widgets.get("job_name")
run_notebook_path = dbutils.widgets.get("run_notebook_path").strip()
workspace_path = dbutils.widgets.get("workspace_path").strip()
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = dbutils.widgets.get("batch_id")
xml_format = dbutils.widgets.get("xml_format") or "com.databricks.spark.xml"
sql_base_path = dbutils.widgets.get("sql_base_path")
spark_version = dbutils.widgets.get("spark_version")
node_type_id = dbutils.widgets.get("node_type_id")
num_workers = int(dbutils.widgets.get("num_workers")) if dbutils.widgets.get("num_workers").strip() else 2
driver_node_type_id = dbutils.widgets.get("driver_node_type_id")
existing_cluster_id = dbutils.widgets.get("existing_cluster_id").strip()
databricks_host = dbutils.widgets.get("databricks_host").strip()
databricks_token = dbutils.widgets.get("databricks_token").strip()
token_secret_scope = dbutils.widgets.get("token_secret_scope").strip()
token_secret_key = dbutils.widgets.get("token_secret_key").strip()

# Resolve notebook path
notebook_path = (workspace_path + "/" + run_notebook_path.lstrip("/")) if workspace_path else run_notebook_path

# COMMAND ----------

# Build job definition (same structure as create_databricks_workflow.py v2_batch)
cluster_config = {
    "spark_version": spark_version,
    "node_type_id": node_type_id,
    "num_workers": num_workers,
    "driver_node_type_id": driver_node_type_id,
    "runtime_engine": "PHOTON",
}

task = {
    "task_key": "run_tpcdi_batch",
    "description": "Run TPC-DI v2 batch pipeline (Bronze → Silver → Gold)",
    "libraries": [{"maven": {"coordinates": "com.databricks:spark-xml_2.13:0.18.0"}}],
    "notebook_task": {
        "notebook_path": notebook_path,
        "base_parameters": {
            "catalog": catalog,
            "schema_name": schema_name,
            "raw_data_path": raw_data_path,
            "sf": sf,
            "batch_id": batch_id,
            "xml_format": xml_format,
            "sql_base_path": sql_base_path or "",
        },
        "source": "WORKSPACE",
    },
    "timeout_seconds": 0,
    "max_retries": 0,
}

if existing_cluster_id:
    task["existing_cluster_id"] = existing_cluster_id
else:
    task["job_cluster_key"] = "run_tpcdi_batch_cluster"

job_clusters = [] if existing_cluster_id else [
    {"job_cluster_key": "run_tpcdi_batch_cluster", "new_cluster": cluster_config},
]

parameters = [
    {"name": "catalog", "default": catalog},
    {"name": "schema_name", "default": schema_name},
    {"name": "raw_data_path", "default": raw_data_path},
    {"name": "sf", "default": sf},
    {"name": "batch_id", "default": batch_id},
    {"name": "xml_format", "default": xml_format},
    {"name": "sql_base_path", "default": sql_base_path or ""},
]

job_payload = {
    "name": job_name,
    "tasks": [task],
    "job_clusters": job_clusters,
    "parameters": parameters,
    "format": "MULTI_TASK",
    "max_concurrent_runs": 1,
    "tags": {"purpose": "tpcdi_benchmark", "component": "v2_batch"},
}

# COMMAND ----------

# Get workspace URL and token
if not databricks_host:
    try:
        databricks_host = spark.conf.get("spark.databricks.workspaceUrl")
        if databricks_host and not databricks_host.startswith("http"):
            databricks_host = "https://" + databricks_host
    except Exception:
        pass
if not databricks_host:
    raise ValueError("Set widget 'databricks_host' (e.g. https://your-workspace.cloud.databricks.com) or run on a cluster with spark.databricks.workspaceUrl set.")

if not databricks_token and token_secret_scope and token_secret_key:
    databricks_token = dbutils.secrets.get(scope=token_secret_scope, key=token_secret_key)
if not databricks_token:
    raise ValueError("Set widget 'databricks_token' (personal access token) or 'token_secret_scope' and 'token_secret_key'.")

# COMMAND ----------

import json
import requests

url = f"{databricks_host.rstrip('/')}/api/2.1/jobs/create"
headers = {"Authorization": f"Bearer {databricks_token}", "Content-Type": "application/json"}

resp = requests.post(url, headers=headers, json=job_payload)
resp.raise_for_status()
result = resp.json()

# COMMAND ----------

job_id = result.get("job_id")
print(f"Job created successfully.")
print(f"  Job ID:   {job_id}")
print(f"  Job name: {job_name}")
print(f"  Notebook: {notebook_path}")
print(f"\nOpen in browser: {databricks_host.rstrip('/')}/#job/{job_id}")

# COMMAND ----------

# Optional: output full job payload for reference
display(json.dumps(job_payload, indent=2))
