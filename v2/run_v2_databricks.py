#!/usr/bin/env python3
"""
Helper for TPC-DI v2 on Databricks (run_tpcdi_batch + sql/).

Usage:
    python run_v2_databricks.py
    python run_v2_databricks.py --guide
"""

import argparse


def print_guide():
    """Print how to run v2 on Databricks (run_tpcdi_batch + create_v2_workflow_notebook)."""
    print("""
================================================================================
TPC-DI v2 on Databricks – run_tpcdi_batch + sql/
================================================================================

You use a single notebook (run_tpcdi_batch) that runs all Bronze → Silver → Gold
via SQL files under sql/ and two Python sub-notebooks for CustomerMgmt/FinWire
and silver customers/accounts.

1. Create the workflow (one-time)
   - Open: v2/databricks/create_v2_workflow_notebook.py
   - Run the notebook to create a Databricks job that runs run_tpcdi_batch
   - Set job parameters: catalog, schema_name, raw_data_path, sf, load_type, etc.

2. Run the pipeline
   - Trigger the job from the UI, or
   - The job runs: v2/databricks/run_tpcdi_batch.py (single task)

3. What run_tpcdi_batch uses
   - sql/bronze/*.sql + sql/bronze/batch/load_bronze_customer_mgmt.py, load_bronze_finwire.py
   - sql/silver/*.sql + sql/silver/batch/transform_silver_customers.py, transform_silver_accounts.py
   - sql/gold/*.sql (load, incremental, optimize)

4. Manual run (notebook)
   - Open run_tpcdi_batch in Databricks (as a notebook)
   - Set widgets: catalog, schema_name, raw_data_path, sf, batch_id, load_type
   - Run all cells

See: v2/databricks/QUICK_START.md, v2/databricks/WORKFLOW_README.md
     v2/RUN_V2.md (full run guide)
================================================================================
""")


def main():
    parser = argparse.ArgumentParser(
        description="TPC-DI v2 Databricks: run_tpcdi_batch + create_v2_workflow_notebook"
    )
    parser.add_argument(
        "--guide",
        action="store_true",
        help="Print execution guide (default if no other action)",
    )
    args = parser.parse_args()
    print_guide()


if __name__ == "__main__":
    main()
