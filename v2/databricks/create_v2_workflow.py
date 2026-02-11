#!/usr/bin/env python3
"""
Create Databricks workflow for TPC-DI v2 SQL-only implementation.

This script creates a workflow with:
- Individual SQL tasks for each table creation
- Separate tasks for batch vs incremental loads
- Proper task dependencies
"""

import json
import argparse
from pathlib import Path
from typing import Dict, Any, List, Tuple


def get_table_files(layer: str) -> List[Tuple[str, Path]]:
    """Get all table creation files for a layer."""
    tables_dir = Path(__file__).parent / layer / "tables"
    if not tables_dir.exists():
        return []
    
    table_files = []
    for sql_file in sorted(tables_dir.glob("create_*.sql")):
        table_name = sql_file.stem.replace("create_", "")
        table_files.append((table_name, sql_file))
    
    return table_files


def create_sql_task(
    task_key: str,
    sql_file_path: str,
    description: str,
    depends_on: List[str] = None,
    variables: Dict[str, str] = None,
    job_cluster_key: str = "default_cluster"
) -> Dict[str, Any]:
    """Create a SQL task definition."""
    task = {
        "task_key": task_key,
        "description": description,
        "job_cluster_key": job_cluster_key,
        "sql_task": {
            "query": {
                "query_id": sql_file_path  # Will be replaced with actual SQL content
            },
            "warehouse_id": None  # Will be set by user
        },
        "timeout_seconds": 0,
        "email_notifications": {},
        "webhook_notifications": {},
        "retry_on_timeout": False,
        "max_retries": 0,
        "min_retry_interval_millis": 0,
        "max_retry_interval_millis": 0,
    }
    
    if depends_on:
        task["depends_on"] = [{"task_key": dep} for dep in depends_on]
    
    if variables:
        task["sql_task"]["parameters"] = [
            {"name": k, "value": v} for k, v in variables.items()
        ]
    
    return task


def create_workflow_definition(
    job_name: str,
    workspace_path: str,
    workflow_type: str = "batch",  # "batch" or "incremental"
    default_catalog: str = "tpcdi_catalog",
    default_bronze_schema: str = "bronze_schema",
    default_silver_schema: str = "silver_schema",
    default_gold_schema: str = "gold_schema",
    default_raw_data_path: str = "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10",
    default_batch_id: int = 1,
    cluster_config: Dict[str, Any] = None,
    warehouse_id: str = None,
) -> Dict[str, Any]:
    """
    Create Databricks workflow definition for v2 SQL implementation.
    
    Args:
        job_name: Name of the workflow job
        workspace_path: Path to SQL files in workspace (e.g., "/Workspace/Repos/org/repo/v2/databricks")
        default_catalog: Unity Catalog name
        default_bronze_schema: Bronze schema name
        default_silver_schema: Silver schema name
        default_gold_schema: Gold schema name
        default_raw_data_path: Path to TPC-DI raw data
        default_batch_id: Default batch ID
        cluster_config: Cluster configuration
        warehouse_id: SQL Warehouse ID (optional, can be set per task)
    """
    if cluster_config is None:
        cluster_config = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
            "driver_node_type_id": "i3.xlarge",
            "runtime_engine": "PHOTON",
        }
    
    base_path = Path(workspace_path)
    tasks = []
    
    # ============================================================================
    # Setup Task: Create Catalog and Schemas
    # ============================================================================
    setup_sql = f"""
CREATE CATALOG IF NOT EXISTS {default_catalog};
USE CATALOG {default_catalog};
CREATE SCHEMA IF NOT EXISTS {default_bronze_schema};
CREATE SCHEMA IF NOT EXISTS {default_silver_schema};
CREATE SCHEMA IF NOT EXISTS {default_gold_schema};
"""
    
    tasks.append({
        "task_key": "00_setup",
        "description": "Create catalog and schemas",
        "job_cluster_key": "default_cluster",
        "sql_task": {
            "query": {
                "query": setup_sql
            },
            "warehouse_id": warehouse_id
        },
        "timeout_seconds": 300,
    })
    
    # ============================================================================
    # Bronze Layer: Table Creation Tasks
    # ============================================================================
    bronze_tables = get_table_files("bronze")
    bronze_create_tasks = []
    
    for table_name, sql_file in bronze_tables:
        relative_path = sql_file.relative_to(Path(__file__).parent)
        sql_file_path = f"{base_path}/{relative_path}"
        
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
                "warehouse_id": warehouse_id,
                "parameters": [
                    {"name": "var.catalog", "value": default_catalog},
                    {"name": "var.bronze_schema", "value": default_bronze_schema},
                ]
            },
            "timeout_seconds": 300,
        })
        bronze_create_tasks.append(task_key)
    
    # ============================================================================
    # Bronze Layer: Load Tasks (Batch vs Incremental)
    # ============================================================================
    bronze_batch_load_task = {
        "task_key": "bronze_load_batch1",
        "description": "Load Bronze Batch 1 data",
        "job_cluster_key": "default_cluster",
        "depends_on": [{"task_key": t} for t in bronze_create_tasks],
        "sql_task": {
            "file": {
                "path": f"{base_path}/bronze/02_load_bronze_batch1.sql"
            },
            "warehouse_id": warehouse_id,
            "parameters": [
                {"name": "var.catalog", "value": default_catalog},
                {"name": "var.bronze_schema", "value": default_bronze_schema},
                {"name": "var.raw_data_path", "value": default_raw_data_path},
                {"name": "var.batch_id", "value": "1"},
            ]
        },
        "timeout_seconds": 3600,
    }
    
    bronze_incremental_load_task = {
        "task_key": "bronze_load_incremental",
        "description": "Load Bronze incremental data",
        "job_cluster_key": "default_cluster",
        "depends_on": [{"task_key": t} for t in bronze_create_tasks],
        "sql_task": {
            "file": {
                "path": f"{base_path}/bronze/03_load_bronze_incremental.sql"
            },
            "warehouse_id": warehouse_id,
            "parameters": [
                {"name": "var.catalog", "value": default_catalog},
                {"name": "var.bronze_schema", "value": default_bronze_schema},
                {"name": "var.raw_data_path", "value": default_raw_data_path},
                {"name": "var.batch_id", "value": str(default_batch_id)},
            ]
        },
        "timeout_seconds": 3600,
    }
    
    # ============================================================================
    # Silver Layer: Table Creation Tasks
    # ============================================================================
    silver_tables = get_table_files("silver")
    silver_create_tasks = []
    
    for table_name, sql_file in silver_tables:
        relative_path = sql_file.relative_to(Path(__file__).parent)
        sql_file_path = f"{base_path}/{relative_path}"
        
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
                "warehouse_id": warehouse_id,
                "parameters": [
                    {"name": "var.catalog", "value": default_catalog},
                    {"name": "var.silver_schema", "value": default_silver_schema},
                ]
            },
            "timeout_seconds": 300,
        })
        silver_create_tasks.append(task_key)
    
    # ============================================================================
    # Silver Layer: Transform Tasks (Batch vs Incremental)
    # ============================================================================
    silver_batch_transform_task = {
        "task_key": "silver_transform_batch1",
        "description": "Transform Bronze → Silver (Batch 1)",
        "job_cluster_key": "default_cluster",
        "depends_on": [
            {"task_key": "bronze_load_batch1"},
            *[{"task_key": t} for t in silver_create_tasks]
        ],
        "sql_task": {
            "file": {
                "path": f"{base_path}/silver/02_transform_silver_batch1.sql"
            },
            "warehouse_id": warehouse_id,
            "parameters": [
                {"name": "var.catalog", "value": default_catalog},
                {"name": "var.bronze_schema", "value": default_bronze_schema},
                {"name": "var.silver_schema", "value": default_silver_schema},
                {"name": "var.batch_id", "value": "1"},
            ]
        },
        "timeout_seconds": 3600,
    }
    
    silver_incremental_transform_task = {
        "task_key": "silver_transform_incremental",
        "description": "Transform Bronze → Silver (Incremental)",
        "job_cluster_key": "default_cluster",
        "depends_on": [
            {"task_key": "bronze_load_incremental"},
            *[{"task_key": t} for t in silver_create_tasks]
        ],
        "sql_task": {
            "file": {
                "path": f"{base_path}/silver/03_transform_silver_incremental.sql"
            },
            "warehouse_id": warehouse_id,
            "parameters": [
                {"name": "var.catalog", "value": default_catalog},
                {"name": "var.bronze_schema", "value": default_bronze_schema},
                {"name": "var.silver_schema", "value": default_silver_schema},
                {"name": "var.batch_id", "value": str(default_batch_id)},
            ]
        },
        "timeout_seconds": 3600,
    }
    
    # ============================================================================
    # Gold Layer: Table Creation Tasks
    # ============================================================================
    gold_tables = get_table_files("gold")
    gold_create_tasks = []
    
    for table_name, sql_file in gold_tables:
        relative_path = sql_file.relative_to(Path(__file__).parent)
        sql_file_path = f"{base_path}/{relative_path}"
        
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
                "warehouse_id": warehouse_id,
                "parameters": [
                    {"name": "var.catalog", "value": default_catalog},
                    {"name": "var.gold_schema", "value": default_gold_schema},
                ]
            },
            "timeout_seconds": 300,
        })
        gold_create_tasks.append(task_key)
    
    # ============================================================================
    # Gold Layer: Load Tasks (Batch vs Incremental)
    # ============================================================================
    gold_batch_load_task = {
        "task_key": "gold_load_batch1",
        "description": "Load Silver → Gold (Batch 1)",
        "job_cluster_key": "default_cluster",
        "depends_on": [
            {"task_key": "silver_transform_batch1"},
            *[{"task_key": t} for t in gold_create_tasks]
        ],
        "sql_task": {
            "file": {
                "path": f"{base_path}/gold/02_load_gold_batch1.sql"
            },
            "warehouse_id": warehouse_id,
            "parameters": [
                {"name": "var.catalog", "value": default_catalog},
                {"name": "var.silver_schema", "value": default_silver_schema},
                {"name": "var.gold_schema", "value": default_gold_schema},
                {"name": "var.batch_id", "value": "1"},
            ]
        },
        "timeout_seconds": 3600,
    }
    
    gold_incremental_load_task = {
        "task_key": "gold_load_incremental",
        "description": "Load Silver → Gold (Incremental)",
        "job_cluster_key": "default_cluster",
        "depends_on": [
            {"task_key": "silver_transform_incremental"},
            *[{"task_key": t} for t in gold_create_tasks]
        ],
        "sql_task": {
            "file": {
                "path": f"{base_path}/gold/03_load_gold_incremental.sql"
            },
            "warehouse_id": warehouse_id,
            "parameters": [
                {"name": "var.catalog", "value": default_catalog},
                {"name": "var.silver_schema", "value": default_silver_schema},
                {"name": "var.gold_schema", "value": default_gold_schema},
                {"name": "var.batch_id", "value": str(default_batch_id)},
            ]
        },
        "timeout_seconds": 3600,
    }
    
    # Add tasks based on workflow type
    if workflow_type == "batch":
        tasks.extend([
            bronze_batch_load_task,
            silver_batch_transform_task,
            gold_batch_load_task,
        ])
    else:  # incremental
        tasks.extend([
            bronze_incremental_load_task,
            silver_incremental_transform_task,
            gold_incremental_load_task,
        ])
    
    # ============================================================================
    # Workflow Definition
    # ============================================================================
    workflow = {
        "name": job_name,
        "email_notifications": {
            "on_success": [],
            "on_failure": [],
            "on_start": [],
        },
        "webhook_notifications": {
            "on_success": [],
            "on_failure": [],
            "on_start": [],
        },
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": tasks,
        "job_clusters": [
            {
                "job_cluster_key": "default_cluster",
                "new_cluster": cluster_config.copy()
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
                "default": str(default_batch_id),
                "description": "Batch ID (for incremental loads)"
            },
            {
                "name": "catalog",
                "default": default_catalog,
                "description": "Unity Catalog name"
            },
            {
                "name": "bronze_schema",
                "default": default_bronze_schema,
                "description": "Bronze schema name"
            },
            {
                "name": "silver_schema",
                "default": default_silver_schema,
                "description": "Silver schema name"
            },
            {
                "name": "gold_schema",
                "default": default_gold_schema,
                "description": "Gold schema name"
            },
            {
                "name": "raw_data_path",
                "default": default_raw_data_path,
                "description": "Path to TPC-DI raw data"
            },
        ],
        "run_as": {
            "user_name": None  # Will use current user
        },
        "tags": {
            "project": "tpcdi",
            "version": "v2",
            "type": "sql-only"
        },
    }
    
    return workflow


def main():
    parser = argparse.ArgumentParser(
        description="Create Databricks workflow for TPC-DI v2 SQL implementation"
    )
    parser.add_argument(
        "--job-name",
        default="TPC-DI-v2-SQL",
        help="Workflow job name (default: TPC-DI-v2-SQL)"
    )
    parser.add_argument(
        "--workspace-path",
        required=True,
        help="Path to SQL files in workspace (e.g., /Workspace/Repos/org/repo/v2/databricks)"
    )
    parser.add_argument(
        "--workflow-type",
        choices=["batch", "incremental"],
        default="batch",
        help="Workflow type: batch or incremental (default: batch)"
    )
    parser.add_argument(
        "--output",
        default="v2_workflow.json",
        help="Output JSON file (default: v2_workflow.json)"
    )
    parser.add_argument(
        "--catalog",
        default="tpcdi_catalog",
        help="Unity Catalog name (default: tpcdi_catalog)"
    )
    parser.add_argument(
        "--bronze-schema",
        default="bronze_schema",
        help="Bronze schema name (default: bronze_schema)"
    )
    parser.add_argument(
        "--silver-schema",
        default="silver_schema",
        help="Silver schema name (default: silver_schema)"
    )
    parser.add_argument(
        "--gold-schema",
        default="gold_schema",
        help="Gold schema name (default: gold_schema)"
    )
    parser.add_argument(
        "--raw-data-path",
        default="/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10",
        help="Path to TPC-DI raw data"
    )
    parser.add_argument(
        "--batch-id",
        type=int,
        default=1,
        help="Default batch ID (default: 1)"
    )
    parser.add_argument(
        "--warehouse-id",
        help="SQL Warehouse ID (optional)"
    )
    
    args = parser.parse_args()
    
    workflow = create_workflow_definition(
        job_name=args.job_name,
        workspace_path=args.workspace_path,
        workflow_type=args.workflow_type,
        default_catalog=args.catalog,
        default_bronze_schema=args.bronze_schema,
        default_silver_schema=args.silver_schema,
        default_gold_schema=args.gold_schema,
        default_raw_data_path=args.raw_data_path,
        default_batch_id=args.batch_id,
        warehouse_id=args.warehouse_id,
    )
    
    with open(args.output, 'w') as f:
        json.dump(workflow, f, indent=2)
    
    print(f"Workflow definition written to: {args.output}")
    print(f"\nWorkflow Summary:")
    print(f"  Job Name: {args.job_name}")
    print(f"  Total Tasks: {len(workflow['tasks'])}")
    print(f"  Bronze Tables: {len(get_table_files('bronze'))}")
    print(f"  Silver Tables: {len(get_table_files('silver'))}")
    print(f"  Gold Tables: {len(get_table_files('gold'))}")
    print(f"\nTo create the workflow:")
    print(f"  databricks jobs create --json-file {args.output}")


if __name__ == "__main__":
    main()
