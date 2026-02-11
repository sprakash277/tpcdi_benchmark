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
dbutils.widgets.text("catalog", "sumit_prakash_benchmark", "Unity Catalog Name")
dbutils.widgets.text("schema_name", "tpcdi_schema", "Schema Name (used for all layers)")
dbutils.widgets.text("sf", "10", "Scale Factor (SF)")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw Data Path (base path, sf will be appended)")
dbutils.widgets.text("batch_id", "1", "Batch ID")
dbutils.widgets.text("metrics_output", "gs://sumit_prakash_gcs/tpcdi/metrics", "Metrics Output Path")
# Note: Notebook tasks use clusters, not SQL warehouses (warehouse_id removed)

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
sf = dbutils.widgets.get("sf")
raw_data_path_base = dbutils.widgets.get("raw_data_path")
batch_id_str = dbutils.widgets.get("batch_id")
metrics_output = dbutils.widgets.get("metrics_output")

# Append sf to schema name and raw data path
schema_name_with_sf = f"{schema_name}_sf{sf}"
raw_data_path = f"{raw_data_path_base}/sf={sf}"

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Workflow Definition

# COMMAND ----------

def get_table_files(layer: str, base_path: Path) -> list:
    """Get all table creation files for a layer."""
    # Convert Path to string for Databricks workspace paths
    base_path_str = str(base_path)
    tables_dir_path = f"{base_path_str}/{layer}/tables"
    print(f"DEBUG: Looking for tables in {tables_dir_path}")
    print(f"DEBUG: base_path = {base_path_str}, layer = {layer}")
    
    table_files = []
    
    # Use Databricks dbutils to list files (works with workspace paths)
    try:
        # List all files in the tables directory
        # dbutils.fs.ls() returns a list of FileInfo objects
        files = dbutils.fs.ls(tables_dir_path)
        print(f"DEBUG: Found {len(files)} total files in {tables_dir_path}")
        
        # Filter for create_* files (both .py and files without extension)
        create_files = [f for f in files if f.name.startswith("create_")]
        print(f"DEBUG: Found {len(create_files)} files starting with 'create_'")
        
        for file_info in sorted(create_files, key=lambda x: x.name):
            file_name = file_info.name
            file_path = file_info.path
            
            # Skip .sql files (they're not used anymore)
            if file_name.endswith('.sql'):
                print(f"DEBUG: Skipping .sql file: {file_name}")
                continue
            
            # Skip directories
            if file_info.isDir():
                print(f"DEBUG: Skipping directory: {file_name}")
                continue
            
            # Extract table name - remove 'create_' prefix and any extension
            if file_name.endswith('.py'):
                table_name = file_name.replace("create_", "").replace(".py", "")
            else:
                # File without extension
                table_name = file_name.replace("create_", "")
            
            # Use the workspace path format (remove file:// prefix if present, keep dbfs:/ if present)
            workspace_path = file_path.replace("file://", "")
            # For workspace paths, we want the path as-is (e.g., /Users/...)
            # Create a Path object for compatibility with existing code
            table_files.append((table_name, Path(workspace_path)))
            print(f"DEBUG: Added table: {table_name} from {file_name} (path: {workspace_path})")
            
    except Exception as e:
        print(f"ERROR: Error reading tables directory {tables_dir_path}: {e}")
        import traceback
        traceback.print_exc()
        
        # Fallback: try Path.glob() in case we're running locally (not in Databricks)
        try:
            tables_dir = base_path / layer / "tables"
            if tables_dir.exists():
                py_files = list(tables_dir.glob("create_*.py"))
                print(f"DEBUG: Fallback - Found {len(py_files)} .py files using Path.glob()")
                for py_file in sorted(py_files):
                    table_name = py_file.stem.replace("create_", "")
                    table_files.append((table_name, py_file))
                    print(f"DEBUG: Added table (fallback): {table_name} from {py_file}")
        except Exception as fallback_error:
            print(f"ERROR: Fallback also failed: {fallback_error}")
    
    if len(table_files) == 0:
        print(f"WARNING: No table files found in {tables_dir_path}")
        print(f"         Expected files matching pattern: create_*.py or create_* (no extension)")
        print(f"         Make sure the files exist in the Databricks workspace at this location")
    
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
                "schema_name": schema_name_with_sf,
            },
            "source": "WORKSPACE"
        },
        "timeout_seconds": 300,
    })
    
    # Bronze table creation tasks
    bronze_tables = get_table_files("bronze", base_path)
    print(f"DEBUG: get_table_files returned {len(bronze_tables)} bronze tables")
    if len(bronze_tables) == 0:
        print(f"ERROR: No bronze table files found! Check that notebook files exist in {base_path}/bronze/tables/")
        print(f"       Expected files matching: create_*.py or create_* (no extension)")
    bronze_create_tasks = []
    
    for table_name, notebook_file in bronze_tables:
        relative_path = notebook_file.relative_to(base_path)
        notebook_file_path = f"{workspace_path}/{relative_path}"
        
        task_key = f"bronze_create_{table_name}"
        # Convert file path to notebook path (remove extension if present for Databricks)
        # Databricks notebooks don't use extensions in their paths
        if notebook_file_path.endswith('.py'):
            notebook_path = notebook_file_path[:-3]  # Remove .py extension
        else:
            notebook_path = notebook_file_path  # Already no extension
        
        tasks.append({
            "task_key": task_key,
            "description": f"Create Bronze table: {table_name}",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": "00_setup"}],
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name_with_sf,
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 300,
        })
        bronze_create_tasks.append(task_key)
    
    # Silver table creation tasks
    silver_tables = get_table_files("silver", base_path)
    print(f"DEBUG: get_table_files returned {len(silver_tables)} silver tables")
    if len(silver_tables) == 0:
        print(f"ERROR: No silver table files found! Check that notebook files exist in {base_path}/silver/tables/")
        print(f"       Expected files matching: create_*.py or create_* (no extension)")
    silver_create_tasks = []
    
    for table_name, notebook_file in silver_tables:
        relative_path = notebook_file.relative_to(base_path)
        notebook_file_path = f"{workspace_path}/{relative_path}"
        
        task_key = f"silver_create_{table_name}"
        # Convert file path to notebook path (remove extension if present for Databricks)
        # Databricks notebooks don't use extensions in their paths
        if notebook_file_path.endswith('.py'):
            notebook_path = notebook_file_path[:-3]  # Remove .py extension
        else:
            notebook_path = notebook_file_path  # Already no extension
        
        tasks.append({
            "task_key": task_key,
            "description": f"Create Silver table: {table_name}",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": "00_setup"}],
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name_with_sf,
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 300,
        })
        silver_create_tasks.append(task_key)
    
    # Gold table creation tasks
    gold_tables = get_table_files("gold", base_path)
    print(f"DEBUG: get_table_files returned {len(gold_tables)} gold tables")
    if len(gold_tables) == 0:
        print(f"ERROR: No gold table files found! Check that notebook files exist in {base_path}/gold/tables/")
        print(f"       Expected files matching: create_*.py or create_* (no extension)")
    gold_create_tasks = []
    
    for table_name, notebook_file in gold_tables:
        relative_path = notebook_file.relative_to(base_path)
        notebook_file_path = f"{workspace_path}/{relative_path}"
        
        task_key = f"gold_create_{table_name}"
        # Convert file path to notebook path (remove extension if present for Databricks)
        # Databricks notebooks don't use extensions in their paths
        if notebook_file_path.endswith('.py'):
            notebook_path = notebook_file_path[:-3]  # Remove .py extension
        else:
            notebook_path = notebook_file_path  # Already no extension
        
        tasks.append({
            "task_key": task_key,
            "description": f"Create Gold table: {table_name}",
            "job_cluster_key": "default_cluster",
            "depends_on": [{"task_key": "00_setup"}],
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name_with_sf,
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 300,
        })
        gold_create_tasks.append(task_key)
    
    # Load/Transform tasks based on workflow type
    if workflow_type == "batch":
        # Bronze load batch 1
        # Dependencies: 00_setup + all bronze table creation tasks
        bronze_load_deps = [{"task_key": "00_setup"}]
        bronze_load_deps.extend([{"task_key": t} for t in bronze_create_tasks])
        print(f"DEBUG: bronze_load_batch1 depends on: {bronze_load_deps}")
        
        tasks.append({
            "task_key": "bronze_load_batch1",
            "description": "Load Bronze Batch 1 data",
            "job_cluster_key": "default_cluster",
            "depends_on": bronze_load_deps,
            "notebook_task": {
                "notebook_path": f"{workspace_path}/bronze/02_load_bronze_batch1",
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name_with_sf,
                    "raw_data_path": raw_data_path,
                    "batch_id": "1",
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        # Bronze individual table metrics tasks
        for table_name in [t[0] for t in bronze_tables]:
            tasks.append({
                "task_key": f"bronze_metrics_{table_name}",
                "description": f"Collect metrics for bronze table: {table_name}",
                "job_cluster_key": "default_cluster",
                "depends_on": [{"task_key": "bronze_load_batch1"}],
                "notebook_task": {
                    "notebook_path": f"{workspace_path}/metrics/collect_table_metrics",
                    "base_parameters": {
                        "catalog": catalog,
                        "schema_name": schema_name_with_sf,
                        "table_name": table_name,
                        "layer": "bronze",
                        "sf": sf,
                        "batch_id": "1",
                        "metrics_output": metrics_output,
                    },
                    "source": "WORKSPACE"
                },
                "timeout_seconds": 300,
            })
        
        # Silver transform batch 1
        silver_transform_deps = [{"task_key": "bronze_load_batch1"}]
        if silver_create_tasks:
            silver_transform_deps.extend([{"task_key": t} for t in silver_create_tasks])
        tasks.append({
            "task_key": "silver_transform_batch1",
            "description": "Transform Bronze → Silver (Batch 1)",
            "job_cluster_key": "default_cluster",
            "depends_on": silver_transform_deps,
            "notebook_task": {
                "notebook_path": f"{workspace_path}/silver/02_transform_silver_batch1",
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name_with_sf,
                    "batch_id": "1",
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        # Silver individual table metrics tasks
        for table_name in [t[0] for t in silver_tables]:
            tasks.append({
                "task_key": f"silver_metrics_{table_name}",
                "description": f"Collect metrics for silver table: {table_name}",
                "job_cluster_key": "default_cluster",
                "depends_on": [{"task_key": "silver_transform_batch1"}],
                "notebook_task": {
                    "notebook_path": f"{workspace_path}/metrics/collect_table_metrics",
                    "base_parameters": {
                        "catalog": catalog,
                        "schema_name": schema_name_with_sf,
                        "table_name": table_name,
                        "layer": "silver",
                        "sf": sf,
                        "batch_id": "1",
                        "metrics_output": metrics_output,
                    },
                    "source": "WORKSPACE"
                },
                "timeout_seconds": 300,
            })
        
        # Gold load batch 1
        gold_load_deps = [{"task_key": "silver_transform_batch1"}]
        if gold_create_tasks:
            gold_load_deps.extend([{"task_key": t} for t in gold_create_tasks])
        tasks.append({
            "task_key": "gold_load_batch1",
            "description": "Load Silver → Gold (Batch 1)",
            "job_cluster_key": "default_cluster",
            "depends_on": gold_load_deps,
            "notebook_task": {
                "notebook_path": f"{workspace_path}/gold/02_load_gold_batch1",
                "base_parameters": {
                    "catalog": catalog,
                    "schema_name": schema_name_with_sf,
                    "batch_id": "1",
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        # Gold individual table metrics tasks
        for table_name in [t[0] for t in gold_tables]:
            tasks.append({
                "task_key": f"gold_metrics_{table_name}",
                "description": f"Collect metrics for gold table: {table_name}",
                "job_cluster_key": "default_cluster",
                "depends_on": [{"task_key": "gold_load_batch1"}],
                "notebook_task": {
                    "notebook_path": f"{workspace_path}/metrics/collect_table_metrics",
                    "base_parameters": {
                        "catalog": catalog,
                        "schema_name": schema_name_with_sf,
                        "table_name": table_name,
                        "layer": "gold",
                        "sf": sf,
                        "batch_id": "1",
                        "metrics_output": metrics_output,
                    },
                    "source": "WORKSPACE"
                },
                "timeout_seconds": 300,
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
                    "schema_name": schema_name_with_sf,
                    "raw_data_path": raw_data_path,
                    "batch_id": str(batch_id),
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        # Bronze individual table metrics tasks (incremental)
        for table_name in [t[0] for t in bronze_tables]:
            tasks.append({
                "task_key": f"bronze_metrics_{table_name}_inc",
                "description": f"Collect metrics for bronze table: {table_name}",
                "job_cluster_key": "default_cluster",
                "depends_on": [{"task_key": "bronze_load_incremental"}],
                "notebook_task": {
                    "notebook_path": f"{workspace_path}/metrics/collect_table_metrics",
                    "base_parameters": {
                        "catalog": catalog,
                        "schema_name": schema_name_with_sf,
                        "table_name": table_name,
                        "layer": "bronze",
                        "sf": sf,
                        "batch_id": str(batch_id),
                        "metrics_output": metrics_output,
                    },
                    "source": "WORKSPACE"
                },
                "timeout_seconds": 300,
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
                    "schema_name": schema_name_with_sf,
                    "batch_id": str(batch_id),
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        # Silver individual table metrics tasks (incremental)
        for table_name in [t[0] for t in silver_tables]:
            tasks.append({
                "task_key": f"silver_metrics_{table_name}_inc",
                "description": f"Collect metrics for silver table: {table_name}",
                "job_cluster_key": "default_cluster",
                "depends_on": [{"task_key": "silver_transform_incremental"}],
                "notebook_task": {
                    "notebook_path": f"{workspace_path}/metrics/collect_table_metrics",
                    "base_parameters": {
                        "catalog": catalog,
                        "schema_name": schema_name_with_sf,
                        "table_name": table_name,
                        "layer": "silver",
                        "sf": sf,
                        "batch_id": str(batch_id),
                        "metrics_output": metrics_output,
                    },
                    "source": "WORKSPACE"
                },
                "timeout_seconds": 300,
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
                    "schema_name": schema_name_with_sf,
                    "batch_id": str(batch_id),
                },
                "source": "WORKSPACE"
            },
            "timeout_seconds": 3600,
        })
        
        # Gold individual table metrics tasks (incremental)
        for table_name in [t[0] for t in gold_tables]:
            tasks.append({
                "task_key": f"gold_metrics_{table_name}_inc",
                "description": f"Collect metrics for gold table: {table_name}",
                "job_cluster_key": "default_cluster",
                "depends_on": [{"task_key": "gold_load_incremental"}],
                "notebook_task": {
                    "notebook_path": f"{workspace_path}/metrics/collect_table_metrics",
                    "base_parameters": {
                        "catalog": catalog,
                        "schema_name": schema_name_with_sf,
                        "table_name": table_name,
                        "layer": "gold",
                        "sf": sf,
                        "batch_id": str(batch_id),
                        "metrics_output": metrics_output,
                    },
                    "source": "WORKSPACE"
                },
                "timeout_seconds": 300,
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
                "name": "sf",
                "default": sf,
                "description": "Scale Factor (SF) - will be appended to schema name and raw data path"
            },
            {
                "name": "raw_data_path",
                "default": raw_data_path_base,
                "description": "Base path to TPC-DI raw data (sf will be appended as /sf={sf})"
            },
            {
                "name": "metrics_output",
                "default": metrics_output,
                "description": "Path to save metrics JSON files"
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

print(f"\n{'='*80}")
print(f"Workflow Definition Generated:")
print(f"{'='*80}")
print(f"  Job Name: {job_name}")
print(f"  Workflow Type: {workflow_type}")
print(f"  Workspace Path: {workspace_path}")
print(f"  Total Tasks: {len(workflow['tasks'])}")
print(f"\n  Task Breakdown:")
bronze_count = len(get_table_files('bronze', Path(workspace_path)))
silver_count = len(get_table_files('silver', Path(workspace_path)))
gold_count = len(get_table_files('gold', Path(workspace_path)))
print(f"    - Bronze Tables Found: {bronze_count}")
print(f"    - Silver Tables Found: {silver_count}")
print(f"    - Gold Tables Found: {gold_count}")

# Count actual tasks by type
task_keys = [t.get('task_key', '') for t in workflow['tasks']]
bronze_create_count = len([k for k in task_keys if k.startswith('bronze_create_')])
silver_create_count = len([k for k in task_keys if k.startswith('silver_create_')])
gold_create_count = len([k for k in task_keys if k.startswith('gold_create_')])
bronze_metrics_count = len([k for k in task_keys if k.startswith('bronze_metrics_')])
silver_metrics_count = len([k for k in task_keys if k.startswith('silver_metrics_')])
gold_metrics_count = len([k for k in task_keys if k.startswith('gold_metrics_')])

print(f"\n  Tasks Added to Workflow:")
print(f"    - Setup tasks: {len([k for k in task_keys if 'setup' in k])}")
print(f"    - Bronze create tasks: {bronze_create_count}")
print(f"    - Bronze load tasks: {len([k for k in task_keys if 'bronze_load' in k])}")
print(f"    - Bronze metrics tasks: {bronze_metrics_count}")
print(f"    - Silver create tasks: {silver_create_count}")
print(f"    - Silver transform tasks: {len([k for k in task_keys if 'silver_transform' in k])}")
print(f"    - Silver metrics tasks: {silver_metrics_count}")
print(f"    - Gold create tasks: {gold_create_count}")
print(f"    - Gold load tasks: {len([k for k in task_keys if 'gold_load' in k])}")
print(f"    - Gold metrics tasks: {gold_metrics_count}")

if bronze_create_count == 0 or silver_create_count == 0 or gold_create_count == 0:
    print(f"\n  ⚠️  WARNING: Missing table creation tasks!")
    print(f"     Expected: {bronze_count} bronze, {silver_count} silver, {gold_count} gold")
    print(f"     Found: {bronze_create_count} bronze, {silver_create_count} silver, {gold_create_count} gold")
    print(f"     Check that notebook files exist in:")
    print(f"       - {workspace_path}/bronze/tables/create_*.py (or create_* without extension)")
    print(f"       - {workspace_path}/silver/tables/create_*.py (or create_* without extension)")
    print(f"       - {workspace_path}/gold/tables/create_*.py (or create_* without extension)")

# Count task types
create_tasks = [t for t in workflow['tasks'] if 'create' in t['task_key']]
metrics_tasks = [t for t in workflow['tasks'] if 'metrics' in t['task_key']]
load_tasks = [t for t in workflow['tasks'] if 'load' in t['task_key'] or 'transform' in t['task_key']]
print(f"\nTask Breakdown:")
print(f"  Create Table Tasks: {len(create_tasks)}")
print(f"  Metrics Tasks: {len(metrics_tasks)}")
print(f"  Load/Transform Tasks: {len(load_tasks)}")
print(f"  Setup Tasks: {len([t for t in workflow['tasks'] if t['task_key'] == '00_setup'])}")

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
