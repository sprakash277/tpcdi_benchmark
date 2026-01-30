#!/usr/bin/env python3
"""
Create Databricks workflow/job for TPC-DI benchmark.
Sequences data generation and benchmark execution with parameterized workflow.
"""

import json
import argparse
from typing import Dict, Any, List


def create_workflow_definition(
    job_name: str,
    data_gen_notebook_path: str,
    benchmark_notebook_path: str,
    default_scale_factor: int = 10,
    default_output_path: str = "dbfs:/mnt/tpcdi",
    default_load_type: str = "batch",
    default_target_database: str = "tpcdi_warehouse",
    default_target_schema: str = "dw",
    default_target_catalog: str = "",
    default_metrics_output: str = "dbfs:/mnt/tpcdi/metrics",
    default_log_detailed_stats: bool = False,
    cluster_config: Dict[str, Any] = None,
) -> Dict[str, Any]:
    """
    Create Databricks workflow definition.
    
    Args:
        job_name: Name of the workflow/job
        data_gen_notebook_path: Path to data generation notebook
        benchmark_notebook_path: Path to benchmark notebook
        default_scale_factor: Default scale factor
        default_output_path: Default TPC-DI raw data path (used by both tasks)
        default_load_type: Default load type (batch/incremental)
        default_target_database: Default target database
        default_target_schema: Default target schema
        default_target_catalog: Default Unity Catalog (optional); when set, create catalog + schema
        default_metrics_output: Default metrics output path
        default_log_detailed_stats: If True, log per-table timing/records; else only job start/end/total duration
        cluster_config: Cluster configuration dict
    
    Returns:
        Workflow definition dictionary
    """
    
    # Default cluster config
    if cluster_config is None:
        cluster_config = {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "i3.xlarge",
            "num_workers": 2,
            "driver_node_type_id": "i3.xlarge",
        }
    
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
                    "notebook_path": data_gen_notebook_path,
                    "base_parameters": {
                        "scale_factor": str(default_scale_factor),
                        "tpcdi_raw_data_path": default_output_path,
                        "upload_threads": "8"
                    }
                },
                "existing_cluster_id": None,
                "new_cluster": cluster_config.copy(),
                "timeout_seconds": 0,
                "email_notifications": {},
                "retry_on_timeout": False,
                "max_retries": 0,
                "min_retry_interval_millis": 0,
                "max_retry_interval_millis": 0,
                "retry_on_timeout": False
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
                    "notebook_path": benchmark_notebook_path,
                    "base_parameters": {
                        "load_type": default_load_type,
                        "scale_factor": str(default_scale_factor),
                        "tpcdi_raw_data_path": default_output_path,
                        "target_database": default_target_database,
                        "target_schema": default_target_schema,
                        "target_catalog": default_target_catalog,
                        "batch_id": "",
                        "metrics_output": default_metrics_output,
                        "log_detailed_stats": "true" if default_log_detailed_stats else "false"
                    }
                },
                "existing_cluster_id": None,
                "new_cluster": cluster_config.copy(),
                "timeout_seconds": 0,
                "email_notifications": {},
                "retry_on_timeout": False,
                "max_retries": 0,
                "min_retry_interval_millis": 0,
                "max_retry_interval_millis": 0,
                "retry_on_timeout": False
            }
        ],
        "parameters": [
            {
                "name": "scale_factor",
                "default": str(default_scale_factor),
                "description": "TPC-DI scale factor (e.g., 10, 100, 1000)"
            },
            {
                "name": "tpcdi_raw_data_path",
                "default": default_output_path,
                "description": "TPC-DI raw data path (used by both 01_data_generation and 02_benchmark_execution); dbfs:/..., /Volumes/..., or gs://..."
            },
            {
                "name": "load_type",
                "default": default_load_type,
                "description": "Load type: batch or incremental"
            },
            {
                "name": "target_database",
                "default": default_target_database,
                "description": "Target database name"
            },
            {
                "name": "target_schema",
                "default": default_target_schema,
                "description": "Target schema name"
            },
            {
                "name": "target_catalog",
                "default": default_target_catalog,
                "description": "Unity Catalog name (optional); when set, create catalog + schema"
            },
            {
                "name": "batch_id",
                "default": "",
                "description": "Batch ID for incremental loads (leave empty for batch)"
            },
            {
                "name": "metrics_output",
                "default": default_metrics_output,
                "description": "Path to save metrics JSON files"
            },
            {
                "name": "log_detailed_stats",
                "default": "true" if default_log_detailed_stats else "false",
                "description": "Log per-table timing and records; false = only job start/end/total duration"
            },
            {
                "name": "upload_threads",
                "default": "8",
                "description": "Number of parallel threads for DBFS uploads"
            }
        ],
        "job_clusters": [],
        "run_as": None,
        "tags": {
            "purpose": "tpcdi_benchmark",
            "component": "data_integration"
        }
    }
    
    return workflow


def create_workflow_via_api(
    workflow_def: Dict[str, Any],
    databricks_host: str,
    databricks_token: str,
    workspace_path: str = None
) -> Dict[str, Any]:
    """
    Create workflow using Databricks Jobs API.
    
    Args:
        workflow_def: Workflow definition dictionary
        databricks_host: Databricks workspace URL (e.g., https://workspace.cloud.databricks.com)
        databricks_token: Databricks personal access token
        workspace_path: Optional workspace path for notebooks
    
    Returns:
        API response with job details
    """
    import requests
    
    # Update notebook paths if workspace_path provided
    if workspace_path:
        for task in workflow_def["tasks"]:
            if "notebook_path" in task.get("notebook_task", {}):
                current_path = task["notebook_task"]["notebook_path"]
                if not current_path.startswith("/"):
                    task["notebook_task"]["notebook_path"] = f"{workspace_path}/{current_path}"
    
    url = f"{databricks_host.rstrip('/')}/api/2.1/jobs/create"
    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, headers=headers, json=workflow_def)
    response.raise_for_status()
    
    return response.json()


def main():
    parser = argparse.ArgumentParser(
        description="Create Databricks workflow for TPC-DI benchmark"
    )
    
    # Workflow configuration
    parser.add_argument("--job-name", default="TPC-DI-Benchmark",
                       help="Name of the Databricks job")
    parser.add_argument("--data-gen-notebook", default="generate_tpcdi_data_notebook",
                       help="Path to data generation notebook (relative to workspace)")
    parser.add_argument("--benchmark-notebook", default="benchmark_databricks_notebook",
                       help="Path to benchmark notebook (relative to workspace)")
    parser.add_argument("--workspace-path", default="/Workspace/Repos",
                       help="Workspace path prefix for notebooks")
    
    # Default parameters
    parser.add_argument("--default-scale-factor", type=int, default=10,
                       help="Default scale factor")
    parser.add_argument("--default-output-path", default="dbfs:/mnt/tpcdi",
                       help="Default TPC-DI raw data path (used by both tasks)")
    parser.add_argument("--default-load-type", default="batch",
                       choices=["batch", "incremental"],
                       help="Default load type")
    parser.add_argument("--default-target-database", default="tpcdi_warehouse",
                       help="Default target database")
    parser.add_argument("--default-target-schema", default="dw",
                       help="Default target schema")
    parser.add_argument("--default-target-catalog", default="",
                       help="Default Unity Catalog (optional); when set, create catalog + schema")
    parser.add_argument("--default-metrics-output", default="dbfs:/mnt/tpcdi/metrics",
                       help="Default metrics output path")
    parser.add_argument("--default-log-detailed-stats", action="store_true",
                       help="Default: log per-table timing/records; else only job start/end/total duration")
    
    # Cluster configuration
    SPARK_VERSIONS = [
        "13.3.x-scala2.12",
        "13.3.x-photon-scala2.12",
        "14.3.x-scala2.12",
        "14.3.x-photon-scala2.12",
        "15.4.x-scala2.12",
        "15.4.x-photon-scala2.12",
        "16.4.x-scala2.12",
        "16.4.x-photon-scala2.12",
    ]
    parser.add_argument("--spark-version", default="14.3.x-scala2.12",
                       choices=SPARK_VERSIONS,
                       help="Databricks Runtime (Spark) version")
    parser.add_argument("--cloud", default="AWS", choices=["AWS", "GCP", "Azure"],
                       help="Cloud (sets default worker/driver node type if --node-type-id not set)")
    parser.add_argument("--node-type-id", default=None,
                       help="Worker node type (default: AWS i3.xlarge, GCP c2-standard-16, Azure Standard_E8s_v3; GCP: n2d-standard-*, c2-standard-*, n2d-highmem-*)")
    parser.add_argument("--driver-node-type-id", default=None,
                       help="Driver node type (default: same as worker for selected cloud)")
    parser.add_argument("--num-workers", type=int, default=2,
                       help="Number of worker nodes")
    parser.add_argument("--use-existing-cluster", 
                       help="Use existing cluster ID instead of creating new")
    
    # API configuration
    parser.add_argument("--databricks-host",
                       help="Databricks workspace URL (e.g., https://workspace.cloud.databricks.com)")
    parser.add_argument("--databricks-token",
                       help="Databricks personal access token")
    parser.add_argument("--output-json", 
                       help="Output workflow definition to JSON file instead of creating via API")
    
    args = parser.parse_args()

    # Default instance types per cloud: (worker, driver)
    # AWS: i3.xlarge; GCP: c2-standard-16 or n2d-highmem-16; Azure: Standard_E8s_v3
    DEFAULT_NODE_TYPES = {
        "AWS": ("i3.xlarge", "i3.xlarge"),           # or i3.2xlarge for SF 100+
        "GCP": ("c2-standard-16", "c2-standard-16"), # or n2d-highmem-16, n2d-standard-16
        "Azure": ("Standard_E8s_v3", "Standard_E8s_v3"),  # or Standard_D8s_v3
    }
    node_type_id = args.node_type_id or DEFAULT_NODE_TYPES[args.cloud][0]
    driver_node_type_id = args.driver_node_type_id or DEFAULT_NODE_TYPES[args.cloud][1]

    # Build cluster config
    cluster_config = {
        "spark_version": args.spark_version,
        "node_type_id": node_type_id,
        "num_workers": args.num_workers,
        "driver_node_type_id": driver_node_type_id,
    }
    
    # Create workflow definition
    workflow = create_workflow_definition(
        job_name=args.job_name,
        data_gen_notebook_path=args.data_gen_notebook,
        benchmark_notebook_path=args.benchmark_notebook,
        default_scale_factor=args.default_scale_factor,
        default_output_path=args.default_output_path,
        default_load_type=args.default_load_type,
        default_target_database=args.default_target_database,
        default_target_schema=args.default_target_schema,
        default_target_catalog=args.default_target_catalog,
        default_metrics_output=args.default_metrics_output,
        default_log_detailed_stats=args.default_log_detailed_stats,
        cluster_config=cluster_config,
    )
    
    # Handle existing cluster
    if args.use_existing_cluster:
        for task in workflow["tasks"]:
            task["existing_cluster_id"] = args.use_existing_cluster
            task.pop("new_cluster", None)
    
    # Output or create via API
    if args.output_json:
        # Save to JSON file
        with open(args.output_json, 'w') as f:
            json.dump(workflow, f, indent=2)
        print(f"Workflow definition saved to {args.output_json}")
        print("\nTo create the job, use:")
        print(f"  databricks jobs create --json-file {args.output_json}")
    elif args.databricks_host and args.databricks_token:
        # Create via API
        try:
            result = create_workflow_via_api(
                workflow,
                args.databricks_host,
                args.databricks_token,
                args.workspace_path
            )
            print(f"✓ Workflow created successfully!")
            print(f"  Job ID: {result.get('job_id')}")
            print(f"  Job Name: {workflow['name']}")
            print(f"\nView job at: {args.databricks_host}/#job/{result.get('job_id')}")
        except Exception as e:
            print(f"✗ Failed to create workflow: {e}")
            print("\nWorkflow definition:")
            print(json.dumps(workflow, indent=2))
            return 1
    else:
        # Just print the definition
        print("Workflow definition (JSON):")
        print(json.dumps(workflow, indent=2))
        print("\nTo create the job:")
        print("  1. Save this JSON to a file")
        print("  2. Use: databricks jobs create --json-file <file>")
        print("  3. Or use --databricks-host and --databricks-token to create via API")
    
    return 0


if __name__ == "__main__":
    exit(main())
