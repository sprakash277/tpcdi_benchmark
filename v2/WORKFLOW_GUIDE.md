# TPC-DI v2 Workflow Guide (run_tpcdi_batch)

## Overview

v2 on Databricks uses **run_tpcdi_batch** (one notebook) to run Bronze → Silver → Gold. All SQL lives under **sql/**; two Python sub-notebooks handle CustomerMgmt/FinWire and silver customers/accounts.

**create_v2_workflow_notebook** creates a Databricks job with a single task: **run_tpcdi_batch**.

## Quick start

### Option 1: Create job via notebook (recommended)

1. Open **v2/databricks/create_v2_workflow_notebook.py** in Databricks.
2. Set widgets: **workspace_path** (e.g. `/Workspace/Repos/org/repo/v2/databricks`), **catalog**, **schema_name**, **raw_data_path**, **sf**, **load_type**, cluster config, etc.
3. Run all cells → job is created.
4. Run the job: Workflows → Jobs → Run now (or `databricks jobs run-now --job-id <id>`).

### Option 2: Run run_tpcdi_batch manually

1. Open **v2/databricks/run_tpcdi_batch.py** as a notebook.
2. Set widgets: **catalog**, **schema_name**, **raw_data_path**, **sf**, **batch_id**, **load_type**.
3. Run all cells.

## What run_tpcdi_batch does

| Phase   | Location              | Contents |
|--------|------------------------|----------|
| Bronze | **sql/bronze/**       | Load SQL (date, time, trade, etc.) + **sql/bronze/batch/** notebooks: `load_bronze_customer_mgmt`, `load_bronze_finwire` |
| Silver | **sql/silver/**       | Transform SQL + **sql/silver/batch/** notebooks: `transform_silver_customers`, `transform_silver_accounts` |
| Gold   | **sql/gold/**         | Load, incremental, optimize, create_gold_dim_messages SQL |

- **load_type = batch**: Full load (Batch 1); run_tpcdi_batch runs bronze SQL + bronze/batch notebooks, silver SQL + silver/batch notebooks, gold load SQL.
- **load_type = incremental**: Batch 2+; run_tpcdi_batch runs sql/bronze/incremental/*.sql, sql/silver/incremental/*.sql, sql/gold/optimize/*.sql, sql/gold/incremental/*.sql.

## File structure

```
v2/databricks/
├── create_v2_workflow_notebook.py   # Creates job that runs run_tpcdi_batch
├── run_tpcdi_batch.py              # Single notebook: Bronze → Silver → Gold
├── tpcdi_metrics.py                # Metrics used by run_tpcdi_batch
└── sql/
    ├── bronze/                     # Bronze SQL + bronze/batch/*.py (customer_mgmt, finwire)
    ├── silver/                     # Silver SQL + silver/batch/*.py (customers, accounts)
    └── gold/                       # Gold SQL (load, incremental, optimize)
```

See **v2/RUN_V2.md** and **v2/databricks/WORKFLOW_README.md** for details.
