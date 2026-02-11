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

# Widgets for workflow configuration
dbutils.widgets.text("job_name", "TPC-DI-v2-SQL", "Job Name")
dbutils.widgets.text("workspace_path", "", "Workspace Path to SQL Files (e.g., /Workspace/Repos/org/repo/v2/databricks)")
dbutils.widgets.dropdown("workflow_type", "batch", ["batch", "incremental"], "Workflow Type")
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog Name")
dbutils.widgets.text("bronze_schema", "bronze_schema", "Bronze Schema")
dbutils.widgets.text("silver_schema", "silver_schema", "Silver Schema")
dbutils.widgets.text("gold_schema", "gold_schema", "Gold Schema")
dbutils.widgets.text("raw_data_path", "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10", "Raw Data Path")
dbutils.widgets.text("batch_id", "1", "Batch ID")
dbutils.widgets.text("warehouse_id", "", "SQL Warehouse ID (required)")

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
dbutils.widgets.dropdown("cloud", "AWS", ["AWS", "GCP", "Azure"], "Cloud")
dbutils.widgets.text("node_type_id", "i3.xlarge", "Worker Node Type")
dbutils.widgets.text("driver_node_type_id", "i3.xlarge", "Driver Node Type")
dbutils.widgets.text("num_workers", "2", "Number of Workers")

# COMMAND ----------

import json
from pathlib import Path

# Get widget values
job_name = dbutils.widgets.get("job_name")
workspace_path = dbutils.widgets.get("workspace_path")
workflow_type = dbutils.widgets.get("workflow_type")
catalog = dbutils.widgets.get("catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_schema = dbutils.widgets.get("silver_schema")
gold_schema = dbutils.widgets.get("gold_schema")
raw_data_path = dbutils.widgets.get("raw_data_path")
batch_id = int(dbutils.widgets.get("batch_id"))
warehouse_id = dbutils.widgets.get("warehouse_id")

spark_version = dbutils.widgets.get("spark_version")
cloud = dbutils.widgets.get("cloud")
node_type_id = dbutils.widgets.get("node_type_id")
driver_node_type_id = dbutils.widgets.get("driver_node_type_id")
num_workers = int(dbutils.widgets.get("num_workers"))

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
    
    # Setup task
    setup_sql = f"""
CREATE CATALOG IF NOT EXISTS {catalog};
USE CATALOG {catalog};
CREATE SCHEMA IF NOT EXISTS {bronze_schema};
CREATE SCHEMA IF NOT EXISTS {silver_schema};
CREATE SCHEMA IF NOT EXISTS {gold_schema};
"""
    
    tasks.append({
        "task_key": "00_setup",
        "description": "Create catalog and schemas",
        "job_cluster_key": "default_cluster",
        "sql_task": {
            "query": {
                "query": setup_sql
            },
            "warehouse_id": warehouse_id if warehouse_id else None
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
        tasks.append({
            "task_key": task_key,
            "description": f"Create Bronze table: {table_name}",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": "00_setup"}],
            "sql_task": {
                "file": {
                    "path": sql_file_path
                },
                "warehouse_id": warehouse_id if warehouse_id else None,
                "parameters": [
                    {"name": "var.catalog", "value": catalog},
                    {"name": "var.bronze_schema", "value": bronze_schema},
                ]
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
        tasks.append({
            "task_key": task_key,
            "description": f"Create Silver table: {table_name}",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": "00_setup"}],
            "sql_task": {
                "file": {
                    "path": sql_file_path
                },
                "warehouse_id": warehouse_id if warehouse_id else None,
                "parameters": [
                    {"name": "var.catalog", "value": catalog},
                    {"name": "var.silver_schema", "value": silver_schema},
                ]
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
        tasks.append({
            "task_key": task_key,
            "description": f"Create Gold table: {table_name}",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": "00_setup"}],
            "sql_task": {
                "file": {
                    "path": sql_file_path
                },
                "warehouse_id": warehouse_id if warehouse_id else None,
                "parameters": [
                    {"name": "var.catalog", "value": catalog},
                    {"name": "var.gold_schema", "value": gold_schema},
                ]
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
            "sql_task": {
                "file": {
                    "path": f"{workspace_path}/bronze/02_load_bronze_batch1.sql"
                },
                "warehouse_id": warehouse_id if warehouse_id else None,
                "parameters": [
                    {"name": "var.catalog", "value": catalog},
                    {"name": "var.bronze_schema", "value": bronze_schema},
                    {"name": "var.raw_data_path", "value": raw_data_path},
                    {"name": "var.batch_id", "value": "1"},
                ]
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
            "sql_task": {
                "file": {
                    "path": f"{workspace_path}/silver/02_transform_silver_batch1.sql"
                },
                "warehouse_id": warehouse_id if warehouse_id else None,
                "parameters": [
                    {"name": "var.catalog", "value": catalog},
                    {"name": "var.bronze_schema", "value": bronze_schema},
                    {"name": "var.silver_schema", "value": silver_schema},
                    {"name": "var.batch_id", "value": "1"},
                ]
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
            "sql_task": {
                "file": {
                    "path": f"{workspace_path}/gold/02_load_gold_batch1.sql"
                },
                "warehouse_id": warehouse_id if warehouse_id else None,
                "parameters": [
                    {"name": "var.catalog", "value": catalog},
                    {"name": "var.silver_schema", "value": silver_schema},
                    {"name": "var.gold_schema", "value": gold_schema},
                    {"name": "var.batch_id", "value": "1"},
                ]
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
            "sql_task": {
                "file": {
                    "path": f"{workspace_path}/bronze/03_load_bronze_incremental.sql"
                },
                "warehouse_id": warehouse_id if warehouse_id else None,
                "parameters": [
                    {"name": "var.catalog", "value": catalog},
                    {"name": "var.bronze_schema", "value": bronze_schema},
                    {"name": "var.raw_data_path", "value": raw_data_path},
                    {"name": "var.batch_id", "value": str(batch_id)},
                ]
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
            "sql_task": {
                "file": {
                    "path": f"{workspace_path}/silver/03_transform_silver_incremental.sql"
                },
                "warehouse_id": warehouse_id if warehouse_id else None,
                "parameters": [
                    {"name": "var.catalog", "value": catalog},
                    {"name": "var.bronze_schema", "value": bronze_schema},
                    {"name": "var.silver_schema", "value": silver_schema},
                    {"name": "var.batch_id", "value": str(batch_id)},
                ]
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
            "sql_task": {
                "file": {
                    "path": f"{workspace_path}/gold/03_load_gold_incremental.sql"
                },
                "warehouse_id": warehouse_id if warehouse_id else None,
                "parameters": [
                    {"name": "var.catalog", "value": catalog},
                    {"name": "var.silver_schema", "value": silver_schema},
                    {"name": "var.gold_schema", "value": gold_schema},
                    {"name": "var.batch_id", "value": str(batch_id)},
                ]
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
                "name": "bronze_schema",
                "default": bronze_schema,
                "description": "Bronze schema name"
            },
            {
                "name": "silver_schema",
                "default": silver_schema,
                "description": "Silver schema name"
            },
            {
                "name": "gold_schema",
                "default": gold_schema,
                "description": "Gold schema name"
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
