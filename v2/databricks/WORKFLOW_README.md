# TPC-DI v2 Databricks Workflow (run_tpcdi_batch)

## Overview

The v2 workflow uses **one notebook**: **run_tpcdi_batch**. It runs Bronze → Silver → Gold using SQL under **sql/** and two Python sub-notebooks for CustomerMgmt/FinWire and silver customers/accounts.

**create_v2_workflow_notebook** creates a Databricks job with a single task that runs **run_tpcdi_batch**.

## Creating the Workflow

1. Open **create_v2_workflow_notebook.py** in Databricks (as a notebook).
2. Set widgets:
   - **job_name**: e.g. `TPC-DI-v2-Batch`
   - **workspace_path**: e.g. `/Workspace/Repos/org/repo/v2/databricks`
   - **catalog**, **schema_name**, **raw_data_path**, **sf**, **batch_id**, **load_type**
   - **cloud**, **num_workers**, **spark_version**, etc.
3. Run the notebook. It creates a job with one task: **run_tpcdi_batch**.

## What run_tpcdi_batch uses

| Layer  | Location | Contents |
|--------|----------|----------|
| Bronze | **sql/bronze/** | Load SQL (date, time, trade, etc.) + **sql/bronze/batch/** notebooks: `load_bronze_customer_mgmt`, `load_bronze_finwire` |
| Silver | **sql/silver/** | Transform SQL + **sql/silver/batch/** notebooks: `transform_silver_customers`, `transform_silver_accounts` |
| Gold   | **sql/gold/**   | Load, incremental, optimize, create_gold_dim_messages SQL |

## Running the Workflow

### Batch load (load_type = batch)

- Trigger the job with **load_type** = `batch`, **batch_id** = `1` (or use defaults).
- run_tpcdi_batch runs bronze SQL + bronze/batch notebooks, then silver SQL + silver/batch notebooks, then gold load SQL.

### Incremental load (load_type = incremental)

- Trigger the job with **load_type** = `incremental`, **batch_id** = `2` (or higher).
- run_tpcdi_batch runs sql/bronze/incremental/*.sql, sql/silver/incremental/*.sql, sql/gold/optimize/*.sql, then sql/gold/incremental/*.sql.

## Parameters (widgets / job parameters)

| Parameter      | Description |
|----------------|-------------|
| catalog        | Unity Catalog name |
| schema_name    | Schema (e.g. tpcdi_sf10) |
| raw_data_path  | Base path to TPC-DI data (run_tpcdi_batch appends /sf={sf}) |
| sf             | Scale factor (e.g. 10) |
| batch_id       | Batch ID (1 for batch, 2+ for incremental) |
| load_type      | `batch` or `incremental` |
| sql_base_path  | Optional; base path for sql/ (default = notebook dir) |

## Troubleshooting

- **Sub-notebook not found**: Ensure **sql/bronze/batch/** and **sql/silver/batch/** exist under the same repo/workspace path as run_tpcdi_batch (or set **sql_base_path**).
- **Table not found**: Check **catalog** and **schema_name**; run_tpcdi_batch creates tables in that schema.
- **Raw data not found**: Verify **raw_data_path** and **sf**; path should be `{raw_data_path}/sf={sf}/` with Batch1/, Batch2/, etc.

See also: **QUICK_START.md**, **v2/RUN_V2.md**.
