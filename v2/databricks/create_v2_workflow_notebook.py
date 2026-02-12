# Databricks notebook source
# MAGIC %md
# MAGIC # Create TPC-DI v2 Workflow (run_tpcdi_batch)
# MAGIC
# MAGIC Creates a Databricks job with one task: **run_tpcdi_batch** (Bronze → Silver → Gold via SQL files).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Instance type options per cloud
CLOUD_NODE_OPTIONS = {
    "AWS": [
        "i3.xlarge", "i3.2xlarge", "i3.4xlarge",
        "m5d.xlarge", "m5d.2xlarge", "m5d.4xlarge",
        "r5d.xlarge", "r5d.2xlarge", "r5d.4xlarge",
    ],
    "GCP": [
        "c2-standard-4", "c2-standard-8", "c2-standard-16", "c2-standard-30",
        "n2d-standard-4", "n2d-standard-8", "n2d-standard-16", "n2d-standard-32",
        "n2d-standard-48", "n2d-standard-64", "n2d-standard-80", "n2d-standard-96",
        "n2d-highmem-4", "n2d-highmem-8", "n2d-highmem-16", "n2d-highmem-32",
        "n2d-highmem-48", "n2d-highmem-64", "n2d-highmem-80", "n2d-highmem-96",
    ],
    "Azure": [
        "Standard_E4s_v3", "Standard_E8s_v3", "Standard_E16s_v3", "Standard_E32s_v3",
        "Standard_D4s_v3", "Standard_D8s_v3", "Standard_D16s_v3", "Standard_D32s_v3",
        "Standard_L4s_v2", "Standard_L8s_v2", "Standard_L16s_v2", "Standard_L32s_v2",
    ],
}
DEFAULT_NODE_TYPES = {
    "AWS": ("m5d.4xlarge", "m5d.4xlarge"),
    "GCP": ("n2d-standard-16", "n2d-standard-16"),
    "Azure": ("Standard_E16s_v3", "Standard_E16s_v3"),
}

def get_worker_count_for_sf(sf: int) -> int:
    if sf == 10:
        return 2
    elif sf == 100:
        return 3
    elif sf == 1000:
        return 5
    return max(2, min(10, sf // 5))

# Pipeline parameters (passed to run_tpcdi_batch)
dbutils.widgets.text("job_name", "TPC-DI-v2-Batch", "Job Name")
dbutils.widgets.text("workspace_path", "", "Workspace Path (e.g. /Workspace/Repos/org/repo/v2/databricks)")
dbutils.widgets.text("catalog", "main", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema", "Schema Name (sf appended as schema_sf10)")
dbutils.widgets.text("sf", "10", "Scale Factor")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw Data Path (base; sf appended)")
dbutils.widgets.text("batch_id", "1", "Batch ID")
dbutils.widgets.dropdown("load_type", "batch", ["batch", "incremental"], "Load Type (batch = full load, incremental = batch 2+)")
dbutils.widgets.text("xml_format", "com.databricks.spark.xml", "XML Format")
dbutils.widgets.text("sql_base_path", "", "SQL base path (optional)")

# Cluster
dbutils.widgets.dropdown(
    "spark_version",
    "14.3.x-scala2.12",
    [
        "13.3.x-scala2.12", "13.3.x-photon-scala2.12",
        "14.3.x-scala2.12", "14.3.x-photon-scala2.12",
        "15.4.x-scala2.12", "15.4.x-photon-scala2.12",
        "16.4.x-scala2.13", "16.4.x-photon-scala2.13",
        "17.3.x-scala2.13", "17.3.x-photon-scala2.13",
    ],
    "Spark Version (DBR)"
)
dbutils.widgets.dropdown("cloud", "AWS", ["AWS", "GCP", "Azure"], "Cloud")
dbutils.widgets.text("num_workers", "", "Number of Workers (blank = from SF)")

# COMMAND ----------

# Update node type dropdowns by cloud
cloud = dbutils.widgets.get("cloud")
options = CLOUD_NODE_OPTIONS.get(cloud, CLOUD_NODE_OPTIONS["AWS"])
default_worker = DEFAULT_NODE_TYPES.get(cloud, ("m5d.4xlarge", "m5d.4xlarge"))[0]
default_driver = DEFAULT_NODE_TYPES.get(cloud, ("m5d.4xlarge", "m5d.4xlarge"))[1]
if default_worker not in options:
    default_worker = options[0]
if default_driver not in options:
    default_driver = options[0]
try:
    dbutils.widgets.remove("node_type_id")
except Exception:
    pass
try:
    dbutils.widgets.remove("driver_node_type_id")
except Exception:
    pass
dbutils.widgets.dropdown("node_type_id", default_worker, options, "Worker Node Type")
dbutils.widgets.dropdown("driver_node_type_id", default_driver, options, "Driver Node Type")
print(f"Cloud: {cloud}; instance types: {len(options)}")

# COMMAND ----------

import json
from pathlib import Path

job_name = dbutils.widgets.get("job_name")
workspace_path = dbutils.widgets.get("workspace_path").strip()
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
sf = dbutils.widgets.get("sf")
raw_data_path_base = dbutils.widgets.get("raw_data_path")
batch_id_str = dbutils.widgets.get("batch_id") or "1"
load_type = dbutils.widgets.get("load_type") or "batch"
xml_format = dbutils.widgets.get("xml_format") or "com.databricks.spark.xml"
sql_base_path = dbutils.widgets.get("sql_base_path") or ""

schema_name_with_sf = f"{schema_name}_sf{sf}"

spark_version = dbutils.widgets.get("spark_version")
cloud = dbutils.widgets.get("cloud")
node_type_id = dbutils.widgets.get("node_type_id")
driver_node_type_id = dbutils.widgets.get("driver_node_type_id")
num_workers_str = dbutils.widgets.get("num_workers").strip()
sf_int = int(sf) if sf else 10
num_workers = int(num_workers_str) if num_workers_str else get_worker_count_for_sf(sf_int)
if not num_workers_str:
    print(f"num_workers={num_workers} (from sf={sf_int})")

if not workspace_path:
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        workspace_path = str(Path(notebook_path).parent)
        print(f"workspace_path: {workspace_path}")
    except Exception:
        workspace_path = "/Workspace/Repos"
        print(f"workspace_path (default): {workspace_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Workflow Definition

# COMMAND ----------

def create_workflow_definition():
    if workspace_path.startswith("/Users/"):
        workspace_path_normalized = workspace_path.replace("/Users/", "/Workspace/Users/", 1)
    elif workspace_path.startswith("/Workspace/"):
        workspace_path_normalized = workspace_path
    else:
        workspace_path_normalized = workspace_path

    run_notebook_path = f"{workspace_path_normalized}/run_tpcdi_batch"
    task = {
        "task_key": "run_tpcdi_batch",
        "description": "Run TPC-DI v2 batch pipeline (Bronze → Silver → Gold)",
        "job_cluster_key": "default_cluster",
        "libraries": [{"maven": {"coordinates": "com.databricks:spark-xml_2.13:0.18.0"}}],
        "notebook_task": {
            "notebook_path": run_notebook_path,
            "base_parameters": {
                "catalog": catalog,
                "schema_name": schema_name_with_sf,
                "raw_data_path": raw_data_path_base,
                "sf": sf,
                "batch_id": batch_id_str,
                "load_type": load_type,
                "xml_format": xml_format,
                "sql_base_path": sql_base_path,
            },
            "source": "WORKSPACE",
        },
        "timeout_seconds": 0,
        "max_retries": 0,
    }
    cluster_config = {
        "spark_version": spark_version,
        "node_type_id": node_type_id,
        "num_workers": num_workers,
        "driver_node_type_id": driver_node_type_id,
    }
    if "photon" in spark_version.lower():
        cluster_config["runtime_engine"] = "PHOTON"

    return {
        "name": job_name,
        "email_notifications": {},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [task],
        "job_clusters": [{"job_cluster_key": "default_cluster", "new_cluster": cluster_config}],
        "format": "MULTI_TASK",
        "parameters": [
            {"name": "catalog", "default": catalog},
            {"name": "schema_name", "default": schema_name_with_sf},
            {"name": "raw_data_path", "default": raw_data_path_base},
            {"name": "sf", "default": sf},
            {"name": "batch_id", "default": batch_id_str},
            {"name": "load_type", "default": load_type},
            {"name": "xml_format", "default": xml_format},
            {"name": "sql_base_path", "default": sql_base_path},
        ],
        "tags": {"project": "tpcdi", "version": "v2", "type": "run_tpcdi_batch"},
    }

workflow = create_workflow_definition()

print(f"Job: {job_name}")
print(f"Notebook: {workspace_path}/run_tpcdi_batch")
print(f"Tasks: 1 (run_tpcdi_batch)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Workflow Job

# COMMAND ----------

import requests

databricks_host = spark.conf.get("spark.databricks.workspaceUrl", "")
if not databricks_host:
    try:
        databricks_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    except Exception:
        pass
if not databricks_host:
    print("ERROR: Could not get workspace URL. Set spark.databricks.workspaceUrl or run in Databricks.")
else:
    if not databricks_host.startswith("http"):
        databricks_host = "https://" + databricks_host
    token = None
    try:
        token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    except Exception:
        pass
    if not token:
        print("ERROR: Could not get API token. Run this notebook in a Databricks context that provides it.")
    else:
        url = f"{databricks_host.rstrip('/')}/api/2.1/jobs/create"
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        try:
            response = requests.post(url, headers=headers, json=workflow)
            response.raise_for_status()
            job_id = response.json()["job_id"]
            print(f"Workflow created.")
            print(f"  Job ID: {job_id}")
            print(f"  Job Name: {job_name}")
            print(f"  View: {databricks_host.rstrip('/')}/#job/{job_id}")
            print(f"  Run: databricks jobs run-now --job-id {job_id}")
        except requests.exceptions.RequestException as e:
            print(f"Error creating workflow: {e}")
            if hasattr(e, "response") and e.response is not None:
                print(e.response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow JSON (for manual creation)

# COMMAND ----------

print(json.dumps(workflow, indent=2))
