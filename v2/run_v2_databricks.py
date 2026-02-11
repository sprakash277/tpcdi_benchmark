#!/usr/bin/env python3
"""
Helper script to execute TPC-DI v2 SQL files in Databricks.

Usage:
    python run_v2_databricks.py --batch 1 --platform databricks
    python run_v2_databricks.py --batch 2 --platform databricks --incremental
"""

import argparse
import os
from pathlib import Path


def read_sql_file(file_path: Path) -> str:
    """Read SQL file and return contents."""
    with open(file_path, 'r') as f:
        return f.read()


def generate_databricks_notebook(batch_id: int, incremental: bool = False):
    """Generate a Databricks notebook with all SQL cells."""
    
    base_path = Path(__file__).parent / "databricks"
    
    cells = []
    
    # Setup cell
    cells.append({
        "cell_type": "sql",
        "source": f"""-- Setup
CREATE CATALOG IF NOT EXISTS tpcdi_catalog;
USE CATALOG tpcdi_catalog;
CREATE SCHEMA IF NOT EXISTS bronze_schema;
CREATE SCHEMA IF NOT EXISTS silver_schema;
CREATE SCHEMA IF NOT EXISTS gold_schema;

SET var.raw_data_path = '/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10';
SET var.batch_id = {batch_id};"""
    })
    
    if not incremental or batch_id == 1:
        # Bronze - Create Tables
        cells.append({
            "cell_type": "sql",
            "source": f"-- Bronze Layer: Create Tables\nUSE SCHEMA bronze_schema;\n\n{read_sql_file(base_path / 'bronze' / '01_create_bronze_tables.sql')}"
        })
        
        # Bronze - Load Batch 1
        cells.append({
            "cell_type": "sql",
            "source": f"-- Bronze Layer: Load Batch {batch_id}\n{read_sql_file(base_path / 'bronze' / '02_load_bronze_batch1.sql')}"
        })
        
        # Silver - Create Tables
        cells.append({
            "cell_type": "sql",
            "source": f"-- Silver Layer: Create Tables\nUSE SCHEMA silver_schema;\n\n{read_sql_file(base_path / 'silver' / '01_create_silver_tables.sql')}"
        })
        
        # Silver - Transform Batch 1
        cells.append({
            "cell_type": "sql",
            "source": f"-- Silver Layer: Transform Batch {batch_id}\n{read_sql_file(base_path / 'silver' / '02_transform_silver_batch1.sql')}"
        })
        
        # Gold - Create Tables
        cells.append({
            "cell_type": "sql",
            "source": f"-- Gold Layer: Create Tables\nUSE SCHEMA gold_schema;\n\n{read_sql_file(base_path / 'gold' / '01_create_gold_tables.sql')}"
        })
        
        # Gold - Load Batch 1
        cells.append({
            "cell_type": "sql",
            "source": f"-- Gold Layer: Load Batch {batch_id}\n{read_sql_file(base_path / 'gold' / '02_load_gold_batch1.sql')}"
        })
    else:
        # Incremental load
        cells.append({
            "cell_type": "sql",
            "source": f"-- Bronze Layer: Incremental Load Batch {batch_id}\nUSE SCHEMA bronze_schema;\n\n{read_sql_file(base_path / 'bronze' / '03_load_bronze_incremental.sql')}"
        })
        
        cells.append({
            "cell_type": "sql",
            "source": f"-- Silver Layer: Incremental Transform Batch {batch_id}\nUSE SCHEMA silver_schema;\n\n{read_sql_file(base_path / 'silver' / '03_transform_silver_incremental.sql')}"
        })
        
        cells.append({
            "cell_type": "sql",
            "source": f"-- Gold Layer: Incremental Load Batch {batch_id}\nUSE SCHEMA gold_schema;\n\n{read_sql_file(base_path / 'gold' / '03_load_gold_incremental.sql')}"
        })
    
    # Verification cell
    cells.append({
        "cell_type": "sql",
        "source": f"""-- Verification
SELECT 'Bronze' AS layer, COUNT(*) AS row_count FROM bronze_customer_mgmt WHERE _batch_id = {batch_id}
UNION ALL
SELECT 'Silver' AS layer, COUNT(*) AS row_count FROM silver_customers WHERE batch_id = {batch_id}
UNION ALL
SELECT 'Gold' AS layer, COUNT(*) AS row_count FROM gold_dim_customer;"""
    })
    
    return cells


def print_execution_guide(batch_id: int, incremental: bool):
    """Print step-by-step execution guide."""
    
    print(f"\n{'='*60}")
    print(f"TPC-DI v2 Execution Guide - Batch {batch_id}")
    print(f"{'='*60}\n")
    
    if incremental and batch_id > 1:
        print("INCREMENTAL LOAD MODE\n")
        print("1. Bronze Layer (Incremental):")
        print("   - Execute: v2/databricks/bronze/03_load_bronze_incremental.sql")
        print("\n2. Silver Layer (Incremental):")
        print("   - Execute: v2/databricks/silver/03_transform_silver_incremental.sql")
        print("\n3. Gold Layer (Incremental):")
        print("   - Execute: v2/databricks/gold/03_load_gold_incremental.sql")
    else:
        print("BATCH 1 (HISTORICAL) LOAD MODE\n")
        print("1. Bronze Layer:")
        print("   - Execute: v2/databricks/bronze/01_create_bronze_tables.sql")
        print("   - Execute: v2/databricks/bronze/02_load_bronze_batch1.sql")
        print("\n2. Silver Layer:")
        print("   - Execute: v2/databricks/silver/01_create_silver_tables.sql")
        print("   - Execute: v2/databricks/silver/02_transform_silver_batch1.sql")
        print("\n3. Gold Layer:")
        print("   - Execute: v2/databricks/gold/01_create_gold_tables.sql")
        print("   - Execute: v2/databricks/gold/02_load_gold_batch1.sql")
    
    print(f"\n{'='*60}")
    print("IMPORTANT: Before executing, set these variables:")
    print(f"  SET var.raw_data_path = '/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10';")
    print(f"  SET var.batch_id = {batch_id};")
    print(f"  USE CATALOG tpcdi_catalog;")
    print(f"  USE SCHEMA bronze_schema;  (change for silver/gold)")
    print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(description='Execute TPC-DI v2 SQL files')
    parser.add_argument('--batch', type=int, default=1, help='Batch ID (default: 1)')
    parser.add_argument('--platform', choices=['databricks', 'dataproc'], 
                       default='databricks', help='Platform (default: databricks)')
    parser.add_argument('--incremental', action='store_true',
                       help='Run incremental load (Batch 2+)')
    parser.add_argument('--generate-notebook', action='store_true',
                       help='Generate Databricks notebook JSON')
    
    args = parser.parse_args()
    
    if args.platform == 'databricks':
        if args.generate_notebook:
            cells = generate_databricks_notebook(args.batch, args.incremental)
            import json
            notebook = {
                "cells": cells,
                "metadata": {
                    "language": "sql",
                    "name": f"TPC-DI v2 Batch {args.batch}"
                },
                "nbformat": 4,
                "nbformat_minor": 0
            }
            output_file = f"tpcdi_v2_batch_{args.batch}.ipynb"
            with open(output_file, 'w') as f:
                json.dump(notebook, f, indent=2)
            print(f"Generated notebook: {output_file}")
            print("Import this file into Databricks to execute all SQL cells.")
        else:
            print_execution_guide(args.batch, args.incremental)
    else:
        print("Dataproc execution guide:")
        print("See v2/RUN_V2.md for detailed Dataproc instructions")
        print("\nKey steps:")
        print("1. Update YOUR_BUCKET in all SQL files")
        print("2. Execute SQL files using spark-sql or Dataproc Jobs API")
        print("3. See RUN_V2.md for complete instructions")


if __name__ == '__main__':
    main()
