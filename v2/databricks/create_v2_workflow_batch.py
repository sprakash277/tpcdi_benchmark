#!/usr/bin/env python3
"""
Create Databricks workflow for TPC-DI v2 SQL-only implementation - BATCH ONLY.

This creates a workflow specifically for batch loads (Batch 1).
For incremental loads, use create_v2_workflow_incremental.py

Usage:
    python create_v2_workflow_batch.py \
        --workspace-path "/Workspace/Repos/org/repo/v2/databricks" \
        --job-name "TPC-DI-v2-Batch" \
        --create-job
"""

import json
import argparse
from pathlib import Path
from typing import Dict, Any, List
from create_v2_workflow import (
    BRONZE_TABLES, SILVER_TABLES, GOLD_TABLES,
    create_sql_query_task, create_workflow_definition
)


def create_batch_workflow(
    job_name: str,
    workspace_path: str,
    default_raw_data_path: str = "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10",
    default_catalog: str = "tpcdi_catalog",
    default_bronze_schema: str = "bronze_schema",
    default_silver_schema: str = "silver_schema",
    default_gold_schema: str = "gold_schema",
    default_batch_id: int = 1,
) -> Dict[str, Any]:
    """Create workflow for batch loads only."""
    
    base_path = Path(workspace_path)
    tasks = []
    
    # Setup task
    setup_sql_content = """-- Setup: Create catalog and schemas
CREATE CATALOG IF NOT EXISTS ${var.catalog};
USE CATALOG ${var.catalog};

CREATE SCHEMA IF NOT EXISTS ${var.bronze_schema};
CREATE SCHEMA IF NOT EXISTS ${var.silver_schema};
CREATE SCHEMA IF NOT EXISTS ${var.gold_schema};

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
        description="Setup: Create catalog/schemas and set variables",
    ))
    
    # Bronze Layer: Create tables
    bronze_create_tasks = []
    for table in BRONZE_TABLES:
        sql_file = base_path / "bronze" / "tables" / f"create_{table}.sql"
        if not sql_file.exists():
            continue
        
        task_key = f"bronze_create_{table}"
        bronze_create_tasks.append(task_key)
        sql_content = sql_file.read_text()
        
        tasks.append(create_sql_query_task(
            task_key=task_key,
            sql_query=sql_content,
            description=f"Create Bronze table: {table}",
            depends_on=["00_setup"],
        ))
    
    # Silver Layer: Create tables
    silver_create_tasks = []
    for table in SILVER_TABLES:
        sql_file = base_path / "silver" / "tables" / f"create_{table}.sql"
        if not sql_file.exists():
            continue
        
        task_key = f"silver_create_{table}"
        silver_create_tasks.append(task_key)
        sql_content = sql_file.read_text()
        
        tasks.append(create_sql_query_task(
            task_key=task_key,
            sql_query=sql_content,
            description=f"Create Silver table: {table}",
            depends_on=["00_setup"] + bronze_create_tasks,
        ))
    
    # Gold Layer: Create tables
    gold_create_tasks = []
    for table in GOLD_TABLES:
        sql_file = base_path / "gold" / "tables" / f"create_{table}.sql"
        if not sql_file.exists():
            continue
        
        task_key = f"gold_create_{table}"
        gold_create_tasks.append(task_key)
        sql_content = sql_file.read_text()
        
        tasks.append(create_sql_query_task(
            task_key=task_key,
            sql_query=sql_content,
            description=f"Create Gold table: {table}",
            depends_on=["00_setup"] + silver_create_tasks,
        ))
    
    # Batch Load tasks only
    bronze_load_batch1_file = base_path / "bronze" / "02_load_bronze_batch1.sql"
    if bronze_load_batch1_file.exists():
        tasks.append(create_sql_query_task(
            task_key="bronze_load_batch1",
            sql_query=bronze_load_batch1_file.read_text(),
            description="Load Bronze Batch 1 data",
            depends_on=bronze_create_tasks,
        ))
    
    silver_transform_batch1_file = base_path / "silver" / "02_transform_silver_batch1.sql"
    if silver_transform_batch1_file.exists():
        tasks.append(create_sql_query_task(
            task_key="silver_transform_batch1",
            sql_query=silver_transform_batch1_file.read_text(),
            description="Transform Bronze → Silver (Batch 1)",
            depends_on=["bronze_load_batch1"] + silver_create_tasks,
        ))
    
    gold_load_batch1_file = base_path / "gold" / "02_load_gold_batch1.sql"
    if gold_load_batch1_file.exists():
        tasks.append(create_sql_query_task(
            task_key="gold_load_batch1",
            sql_query=gold_load_batch1_file.read_text(),
            description="Load Silver → Gold (Batch 1)",
            depends_on=["silver_transform_batch1"] + gold_create_tasks,
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
            "default": "1",
            "description": "Batch ID (should be 1 for batch load)"
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
    parser = argparse.ArgumentParser(description="Create Databricks workflow for TPC-DI v2 (Batch)")
    parser.add_argument("--job-name", default="TPC-DI-v2-Batch", help="Job name")
    parser.add_argument("--workspace-path", required=True, help="Workspace path to v2/databricks folder")
    parser.add_argument("--output", default="v2_workflow_batch.json", help="Output JSON file")
    parser.add_argument("--databricks-host", help="Databricks workspace URL")
    parser.add_argument("--databricks-token", help="Databricks API token")
    parser.add_argument("--create-job", action="store_true", help="Create job via API")
    
    args = parser.parse_args()
    
    workflow = create_batch_workflow(
        job_name=args.job_name,
        workspace_path=args.workspace_path,
    )
    
    with open(args.output, 'w') as f:
        json.dump(workflow, f, indent=2)
    
    print(f"Batch workflow saved to: {args.output}")
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
