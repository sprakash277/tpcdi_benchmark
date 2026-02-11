# Databricks notebook source
# MAGIC %md
# MAGIC # Create TPC-DI v2 SQL Workflow
# MAGIC
# MAGIC This notebook creates a Databricks workflow for the SQL-only v2 implementation.
# MAGIC
# MAGIC Features:
# MAGIC - Individual SQL tasks for each table creation
# MAGIC - Separate batch and incremental workflows
# MAGIC - Proper task dependencies
# MAGIC - Configurable via widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Configure workflow parameters below.

# COMMAND ----------

# Instance type options per cloud (only these are valid for each cloud)
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
    "AWS": ("m5d.4xlarge", "m5d.4xlarge"),  # GCP equivalent: n2d-standard-16 (16 vCPUs, 64 GB RAM)
    "GCP": ("n2d-standard-16", "n2d-standard-16"),
    "Azure": ("Standard_E16s_v3", "Standard_E16s_v3"),  # GCP equivalent: n2d-standard-16 (16 vCPUs, 128 GB RAM)
}

def get_worker_count_for_scale_factor(scale_factor: int) -> int:
    """Get recommended number of worker nodes based on scale factor."""
    if scale_factor == 10:
        return 2
    elif scale_factor == 100:
        return 3
    elif scale_factor == 1000:
        return 5
    else:
        # Default: scale_factor / 5, minimum 2, maximum 10
        return max(2, min(10, scale_factor // 5))

# Widgets for workflow configuration
dbutils.widgets.text("job_name", "TPC-DI-v2-SQL", "Job Name")
dbutils.widgets.text("workspace_path", "", "Workspace Path to SQL Files (e.g., /Workspace/Repos/org/repo/v2/databricks)")
dbutils.widgets.dropdown("workflow_type", "batch", ["batch", "incremental"], "Workflow Type")
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog Name")
dbutils.widgets.text("schema_name", "tpcdi_schema", "Schema Name (used for all layers)")
dbutils.widgets.text("raw_data_path", "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10", "Raw Data Path")
dbutils.widgets.text("batch_id", "1", "Batch ID")
# Note: Notebook tasks use clusters, not SQL warehouses

# Cluster configuration
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
        "16.4.x-scala2.13",
        "16.4.x-photon-scala2.13",
        "17.3.x-scala2.13",
        "17.3.x-photon-scala2.13",
    ],
    "Cluster Spark Version (DBR)"
)
dbutils.widgets.dropdown("cloud", "AWS", ["AWS", "GCP", "Azure"], "Cloud (pick first; then re-run next cell for instance types)")
dbutils.widgets.text("scale_factor", "10", "Scale Factor (SF=10→2 workers, SF=100→3 workers, SF=1000→5 workers)")
dbutils.widgets.text("num_workers", "2", "Number of Workers (auto-set based on scale_factor if left blank)")

# COMMAND ----------

# Re-run this cell after changing Cloud to update Worker/Driver dropdowns to that cloud's instance types only
cloud = dbutils.widgets.get("cloud")
options = CLOUD_NODE_OPTIONS.get(cloud, CLOUD_NODE_OPTIONS["AWS"])
default_worker = DEFAULT_NODE_TYPES.get(cloud, ("m5d.4xlarge", "m5d.4xlarge"))[0]
default_driver = DEFAULT_NODE_TYPES.get(cloud, ("m5d.4xlarge", "m5d.4xlarge"))[1]
# Ensure defaults are in the options list
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
dbutils.widgets.dropdown("node_type_id", default_worker, options, "Worker Node Type (" + cloud + ")")
dbutils.widgets.dropdown("driver_node_type_id", default_driver, options, "Driver Node Type (" + cloud + ")")
print(f"Instance type options updated for cloud: {cloud} ({len(options)} types)")

# COMMAND ----------

import json
from pathlib import Path

# Get widget values
job_name = dbutils.widgets.get("job_name")
workspace_path = dbutils.widgets.get("workspace_path")
workflow_type = dbutils.widgets.get("workflow_type")
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
batch_id_str = dbutils.widgets.get("batch_id")
warehouse_id = dbutils.widgets.get("warehouse_id")

spark_version = dbutils.widgets.get("spark_version")
cloud = dbutils.widgets.get("cloud")
node_type_id = dbutils.widgets.get("node_type_id")
driver_node_type_id = dbutils.widgets.get("driver_node_type_id")
scale_factor_str = dbutils.widgets.get("scale_factor")
num_workers_str = dbutils.widgets.get("num_workers")

# Parse batch_id
batch_id = int(batch_id_str) if batch_id_str else 1

# Parse scale_factor and auto-calculate num_workers if not provided
scale_factor = int(scale_factor_str) if scale_factor_str else 10
if num_workers_str and num_workers_str.strip():
    num_workers = int(num_workers_str)
else:
    # Auto-calculate based on scale factor
    num_workers = get_worker_count_for_scale_factor(scale_factor)
    print(f"Auto-setting num_workers={num_workers} based on scale_factor={scale_factor}")

# Use schema_name for all layers
bronze_schema = schema_name
silver_schema = schema_name
gold_schema = schema_name

# Auto-detect workspace path if not provided
if not workspace_path:
    try:
        notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        workspace_path = str(Path(notebook_path).parent)
        print(f"Auto-detected workspace_path: {workspace_path}")
    except Exception:
        workspace_path = "/Workspace/Repos"
        print(f"Could not auto-detect workspace_path, using default: {workspace_path}")

if not warehouse_id:
    print("WARNING: warehouse_id not set. You must set it before creating the workflow.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Workflow Definition

# COMMAND ----------

def get_table_files(layer: str, base_path: Path) -> list:
    """Get all table creation files for a layer."""
    tables_dir = base_path / layer / "tables"
    if not tables_dir.exists():
        return []
    
    table_files = []
    for sql_file in sorted(tables_dir.glob("create_*.sql")):
        table_name = sql_file.stem.replace("create_", "")
        table_files.append((table_name, sql_file))
    
    return table_files

def create_workflow_definition():
    """Create workflow definition based on widget values."""
    
    base_path = Path(workspace_path)
    tasks = []
    
    # Setup task - using notebook
    setup_notebook_path = f"{workspace_path}/00_setup"
    
    tasks.append({
        "task_key": "00_setup",
        "description": "Create catalog and schemas",
        "job_cluster_key": "default_cluster",
        "notebook_task": {
            "notebook_path": setup_notebook_path,
            "base_parameters": {
                "catalog": catalog,
                "schema_name": schema_name,
            },
            "source": "WORKSPACE"
        },
        "timeout_seconds": 300,
    })
    
    # Bronze table creation tasks
    bronze_tables = get_table_files("bronze", base_path)
    bronze_create_tasks = []
    
    for table_name, sql_file in bronze_tables:
        relative_path = sql_file.relative_to(base_path)
        sql_file_path = f"{workspace_path}/{relative_path}"
        
        task_key = f"bronze_create_{table_name}"
        # Convert SQL file path to notebook path (assume notebooks are in notebooks/ subdirectory)
        notebook_path = sql_file_path.replace("/tables/create_", "/notebooks/create_").replace(".sql", "")
        
        tasks.append({
            "task_key": task_key,
            "description": f"Create Bronze table: {table_name}",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": "00_setup"}],
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name,
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 300,
        })
        bronze_create_tasks.append(task_key)
    
    # Silver table creation tasks
    silver_tables = get_table_files("silver", base_path)
    silver_create_tasks = []
    
    for table_name, sql_file in silver_tables:
        relative_path = sql_file.relative_to(base_path)
        sql_file_path = f"{workspace_path}/{relative_path}"
        
        task_key = f"silver_create_{table_name}"
        # Convert SQL file path to notebook path
        notebook_path = sql_file_path.replace("/tables/create_", "/notebooks/create_").replace(".sql", "")
        
        tasks.append({
            "task_key": task_key,
            "description": f"Create Silver table: {table_name}",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": "00_setup"}],
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name,
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 300,
        })
        silver_create_tasks.append(task_key)
    
    # Gold table creation tasks
    gold_tables = get_table_files("gold", base_path)
    gold_create_tasks = []
    
    for table_name, sql_file in gold_tables:
        relative_path = sql_file.relative_to(base_path)
        sql_file_path = f"{workspace_path}/{relative_path}"
        
        task_key = f"gold_create_{table_name}"
        # Convert SQL file path to notebook path
        notebook_path = sql_file_path.replace("/tables/create_", "/notebooks/create_").replace(".sql", "")
        
        tasks.append({
            "task_key": task_key,
            "description": f"Create Gold table: {table_name}",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": "00_setup"}],
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name,
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 300,
        })
        gold_create_tasks.append(task_key)
    
    # Load/Transform tasks based on workflow type
    if workflow_type == "batch":
        # Bronze load batch 1
        tasks.append({
            "task_key": "bronze_load_batch1",
            "description": "Load Bronze Batch 1 data",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": t} for t in bronze_create_tasks],
            "notebook_task": {
                "notebook_path": f"{workspace_path}/bronze/02_load_bronze_batch1",
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name,
                    "raw_data_path": raw_data_path,
                    "batch_id": "1",
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        # Silver transform batch 1
        tasks.append({
            "task_key": "silver_transform_batch1",
            "description": "Transform Bronze → Silver (Batch 1)",
            "job_cluster_key": "default_cluster",
            "depends_on": [
                {"task_key": "bronze_load_batch1"},
                *[{"task_key": t} for t in silver_create_tasks]
            ],
            "notebook_task": {
                "notebook_path": f"{workspace_path}/silver/02_transform_silver_batch1",
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name,
                    "batch_id": "1",
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        # Gold load batch 1
        tasks.append({
            "task_key": "gold_load_batch1",
            "description": "Load Silver → Gold (Batch 1)",
            "job_cluster_key": "default_cluster",
            "depends_on": [
                {"task_key": "silver_transform_batch1"},
                *[{"task_key": t} for t in gold_create_tasks]
            ],
            "notebook_task": {
                "notebook_path": f"{workspace_path}/gold/02_load_gold_batch1",
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name,
                    "batch_id": "1",
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
    else:
        # Incremental workflow
        tasks.append({
            "task_key": "bronze_load_incremental",
            "description": "Load Bronze incremental data",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": t} for t in bronze_create_tasks],
            "notebook_task": {
                "notebook_path": f"{workspace_path}/bronze/03_load_bronze_incremental",
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name,
                    "raw_data_path": raw_data_path,
                    "batch_id": str(batch_id),
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        tasks.append({
            "task_key": "silver_transform_incremental",
            "description": "Transform Bronze → Silver (Incremental)",
            "job_cluster_key": "default_cluster",
            "depends_on": [
                {"task_key": "bronze_load_incremental"},
                *[{"task_key": t} for t in silver_create_tasks]
            ],
            "notebook_task": {
                "notebook_path": f"{workspace_path}/silver/03_transform_silver_incremental",
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name,
                    "batch_id": str(batch_id),
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        tasks.append({
            "task_key": "gold_load_incremental",
            "description": "Load Silver → Gold (Incremental)",
            "job_cluster_key": "default_cluster",
            "depends_on": [
                {"task_key": "silver_transform_incremental"},
                *[{"task_key": t} for t in gold_create_tasks]
            ],
            "notebook_task": {
                "notebook_path": f"{workspace_path}/gold/03_load_gold_incremental",
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name,
                    "batch_id": str(batch_id),
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
    
    # Cluster configuration
    cluster_config = {
        "spark_version": spark_version,
        "node_type_id": node_type_id,
        "num_workers": num_workers,
        "driver_node_type_id": driver_node_type_id,
    }
    
    # Add runtime engine if Photon
    if "photon" in spark_version.lower():
        cluster_config["runtime_engine"] = "PHOTON"
    
    # Workflow definition
    workflow = {
        "name": job_name,
        "email_notifications": {},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": tasks,
        "job_clusters": [
            {
                "job_cluster_key": "default_cluster",
                "new_cluster": cluster_config
            }
        ],
        "format": "MULTI_TASK",
        "parameters": [
            {
                "name": "load_type",
                "default": workflow_type,
                "description": "Load type: batch or incremental"
            },
            {
                "name": "batch_id",
                "default": str(batch_id),
                "description": "Batch ID"
            },
            {
                "name": "catalog",
                "default": catalog,
                "description": "Unity Catalog name"
            },
            {
                "name": "schema_name",
                "default": schema_name,
                "description": "Schema name (used for all layers: bronze, silver, gold)"
            },
            {
                "name": "raw_data_path",
                "default": raw_data_path,
                "description": "Path to TPC-DI raw data"
            },
        ],
        "tags": {
            "project": "tpcdi",
            "version": "v2",
            "type": "sql-only"
        },
    }
    
    return workflow

# COMMAND ----------

# Generate workflow
workflow = create_workflow_definition()

print(f"Workflow Definition Generated:")
print(f"  Job Name: {job_name}")
print(f"  Workflow Type: {workflow_type}")
print(f"  Total Tasks: {len(workflow['tasks'])}")
print(f"  Bronze Tables: {len(get_table_files('bronze', Path(workspace_path)))}")
print(f"  Silver Tables: {len(get_table_files('silver', Path(workspace_path)))}")
print(f"  Gold Tables: {len(get_table_files('gold', Path(workspace_path)))}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Workflow Job

# COMMAND ----------

import requests

databricks_host = spark.conf.get("spark.databricks.workspaceUrl", "")
if not databricks_host:
    databricks_host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()

if not databricks_host:
    print("ERROR: Could not determine Databricks workspace URL")
    print("Please set spark.databricks.workspaceUrl or run this notebook in Databricks")
else:
    # Get token from context
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    
    url = f"https://{databricks_host}/api/2.1/jobs/create"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(url, headers=headers, json=workflow)
        response.raise_for_status()
        
        job_id = response.json()["job_id"]
        print(f"\n✅ Workflow created successfully!")
        print(f"   Job ID: {job_id}")
        print(f"   Job Name: {job_name}")
        print(f"\n   View job: https://{databricks_host}/#job/{job_id}")
        print(f"\n   To run the workflow:")
        print(f"   databricks jobs run-now --job-id {job_id}")
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Error creating workflow: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response: {e.response.text}")
        print(f"\n   Workflow JSON saved below - you can create it manually via UI or CLI")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workflow JSON (for manual creation)

# COMMAND ----------

# Display workflow JSON for manual creation if needed
print(json.dumps(workflow, indent=2))
