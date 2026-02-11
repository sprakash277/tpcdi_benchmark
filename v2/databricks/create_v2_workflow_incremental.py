#!/usr/bin/env python3
"""
Create Databricks workflow for TPC-DI v2 SQL-only implementation - INCREMENTAL ONLY.

This creates a workflow specifically for incremental loads (Batch 2+).
For batch loads, use create_v2_workflow_batch.py

Usage:
    python create_v2_workflow_incremental.py \
        --workspace-path "/Workspace/Repos/org/repo/v2/databricks" \
        --job-name "TPC-DI-v2-Incremental" \
        --create-job
"""

import json
import argparse
from pathlib import Path
from typing import Dict, Any
from create_v2_workflow import (
    BRONZE_TABLES, SILVER_TABLES, GOLD_TABLES,
    create_sql_query_task
)


def create_incremental_workflow(
    job_name: str,
    workspace_path: str,
    default_raw_data_path: str = "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10",
    default_catalog: str = "tpcdi_catalog",
    default_bronze_schema: str = "bronze_schema",
    default_silver_schema: str = "silver_schema",
    default_gold_schema: str = "gold_schema",
    default_batch_id: int = 2,
) -> Dict[str, Any]:
    """Create workflow for incremental loads only."""
    
    base_path = Path(workspace_path)
    tasks = []
    
    # Setup task
    setup_sql_content = """-- Setup: Set variables (tables should already exist)
USE CATALOG ${var.catalog};
USE SCHEMA ${var.bronze_schema};

-- Set variables
SET var.raw_data_path = '${var.raw_data_path}';
SET var.batch_id = ${var.batch_id};
SET var.catalog = '${var.catalog}';
SET var.bronze_schema = '${var.bronze_schema}';
SET var.silver_schema = '${var.silver_schema}';
SET var.gold_schema = '${var.gold_schema}';
"""
    
    tasks.append(create_sql_query_task(
        task_key="00_setup",
        sql_query=setup_sql_content,
        description="Setup: Set variables for incremental load",
    ))
    
    # Incremental Load tasks only (tables already exist from Batch 1)
    bronze_load_inc_file = base_path / "bronze" / "03_load_bronze_incremental.sql"
    if bronze_load_inc_file.exists():
        tasks.append(create_sql_query_task(
            task_key="bronze_load_incremental",
            sql_query=bronze_load_inc_file.read_text(),
            description="Load Bronze incremental data",
            depends_on=["00_setup"],
        ))
    
    silver_transform_inc_file = base_path / "silver" / "03_transform_silver_incremental.sql"
    if silver_transform_inc_file.exists():
        tasks.append(create_sql_query_task(
            task_key="silver_transform_incremental",
            sql_query=silver_transform_inc_file.read_text(),
            description="Transform Bronze → Silver (Incremental)",
            depends_on=["bronze_load_incremental"],
        ))
    
    gold_load_inc_file = base_path / "gold" / "03_load_gold_incremental.sql"
    if gold_load_inc_file.exists():
        tasks.append(create_sql_query_task(
            task_key="gold_load_incremental",
            sql_query=gold_load_inc_file.read_text(),
            description="Load Silver → Gold (Incremental)",
            depends_on=["silver_transform_incremental"],
        ))
    
    # Parameters
    parameters = [
        {
            "name": "raw_data_path",
            "default": default_raw_data_path,
            "description": "TPC-DI raw data path"
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
            "name": "batch_id",
            "default": str(default_batch_id),
            "description": "Batch ID (2+ for incremental)"
        },
        {
            "name": "warehouse_id",
            "default": "",
            "description": "SQL Warehouse ID (required)"
        },
    ]
    
    workflow = {
        "name": job_name,
        "email_notifications": {},
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": tasks,
        "parameters": parameters,
        "format": "MULTI_TASK",
    }
    
    return workflow


def main():
    parser = argparse.ArgumentParser(description="Create Databricks workflow for TPC-DI v2 (Incremental)")
    parser.add_argument("--job-name", default="TPC-DI-v2-Incremental", help="Job name")
    parser.add_argument("--workspace-path", required=True, help="Workspace path to v2/databricks folder")
    parser.add_argument("--output", default="v2_workflow_incremental.json", help="Output JSON file")
    parser.add_argument("--databricks-host", help="Databricks workspace URL")
    parser.add_argument("--databricks-token", help="Databricks API token")
    parser.add_argument("--create-job", action="store_true", help="Create job via API")
    
    args = parser.parse_args()
    
    workflow = create_incremental_workflow(
        job_name=args.job_name,
        workspace_path=args.workspace_path,
    )
    
    with open(args.output, 'w') as f:
        json.dump(workflow, f, indent=2)
    
    print(f"Incremental workflow saved to: {args.output}")
    print(f"Total tasks: {len(workflow['tasks'])}")
    
    if args.create_job and args.databricks_host and args.databricks_token:
        import requests
        url = f"{args.databricks_host}/api/2.1/jobs/create"
        headers = {
            "Authorization": f"Bearer {args.databricks_token}",
            "Content-Type": "application/json"
        }
        response = requests.post(url, headers=headers, json=workflow)
        if response.status_code == 200:
            job_id = response.json()["job_id"]
            print(f"Job created! Job ID: {job_id}")
        else:
            print(f"Error: {response.status_code}")
            print(response.text)


if __name__ == "__main__":
    main()
