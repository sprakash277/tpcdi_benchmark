# Quick Start: run_tpcdi_batch + create_v2_workflow_notebook

## Overview

v2 on Databricks uses **one notebook** (**run_tpcdi_batch**) that runs Bronze → Silver → Gold using SQL under **sql/** and two Python sub-notebooks for CustomerMgmt/FinWire and silver customers/accounts.

**create_v2_workflow_notebook** creates a Databricks job with a single task: **run_tpcdi_batch**.

## File structure

```
v2/databricks/
├── create_v2_workflow_notebook.py   # Creates job → run_tpcdi_batch
├── run_tpcdi_batch.py               # Main notebook (Bronze → Silver → Gold)
├── tpcdi_metrics.py                 # Metrics/reporting
└── sql/
    ├── bronze/                      # Bronze SQL + batch/ (customer_mgmt, finwire .py)
    ├── silver/                      # Silver SQL + batch/ (customers, accounts .py)
    └── gold/                        # Gold SQL (load, incremental, optimize)
```

## Step 1: Create the job

1. Open **create_v2_workflow_notebook.py** in Databricks (as a notebook).
2. Set widgets: **job_name**, **workspace_path**, **catalog**, **schema_name**, **raw_data_path**, **sf**, **load_type**, **cloud**, **num_workers**, etc.
3. Run all cells. The notebook creates a job with one task: **run_tpcdi_batch**.

## Step 2: Run the pipeline

- **Via UI**: Workflows → Jobs → select the job → Run now.
- **Via CLI**: `databricks jobs run-now --job-id <job-id>`

Or run **run_tpcdi_batch** manually: open it as a notebook, set widgets (catalog, schema_name, raw_data_path, sf, batch_id, load_type), run all cells.

## Parameters (widgets / job params)

| Parameter     | Description |
|---------------|-------------|
| catalog       | Unity Catalog name |
| schema_name   | Schema (e.g. tpcdi_sf10) |
| raw_data_path | Base path to TPC-DI data (run_tpcdi_batch appends /sf={sf}) |
| sf            | Scale factor (e.g. 10) |
| batch_id      | Batch ID (1 for batch, 2+ for incremental) |
| load_type     | `batch` or `incremental` |

## Verification

```sql
USE CATALOG your_catalog;
USE SCHEMA your_schema;

SELECT COUNT(*) FROM bronze_trade;
SELECT COUNT(*) FROM silver_trades;
SELECT COUNT(*) FROM gold_fact_trade;
```

See **WORKFLOW_README.md** and **v2/RUN_V2.md** for more.
