# TPC-DI v2 Implementation Guide (run_tpcdi_batch + sql/)

## Overview

v2 on Databricks is a SQL-based pipeline driven by **run_tpcdi_batch** (one notebook). It runs Bronze → Silver → Gold using SQL under **sql/** and two Python sub-notebooks for CustomerMgmt/FinWire and silver customers/accounts.

## Directory structure

```
v2/
├── databricks/
│   ├── create_v2_workflow_notebook.py   # Creates Databricks job → run_tpcdi_batch
│   ├── run_tpcdi_batch.py               # Main notebook (Bronze → Silver → Gold)
│   ├── tpcdi_metrics.py                 # Metrics/reporting
│   └── sql/
│       ├── bronze/                      # Bronze SQL + batch/ (customer_mgmt, finwire .py)
│       ├── silver/                      # Silver SQL + batch/ (customers, accounts .py)
│       └── gold/                        # Gold SQL (load, incremental, optimize)
├── run_v2_databricks.py                 # Helper: print execution guide
├── RUN_V2.md                            # Full run instructions
└── README.md                            # Overview
```

## Execution

### Batch (load_type = batch, batch_id = 1)

1. **Bronze**: run_tpcdi_batch runs **sql/bronze/** load SQL and **sql/bronze/batch/** notebooks (load_bronze_customer_mgmt, load_bronze_finwire).
2. **Silver**: Runs **sql/silver/** transform SQL and **sql/silver/batch/** notebooks (transform_silver_customers, transform_silver_accounts).
3. **Gold**: Runs **sql/gold/** load SQL.

### Incremental (load_type = incremental, batch_id ≥ 2)

1. **Bronze**: run_tpcdi_batch runs **sql/bronze/incremental/*.sql**.
2. **Silver**: Runs **sql/silver/incremental/*.sql**.
3. **Gold**: Runs **sql/gold/optimize/*.sql**, then **sql/gold/incremental/*.sql** (and create_gold_dim_messages if needed).

## Configuration

Set via run_tpcdi_batch widgets (or job parameters when using create_v2_workflow_notebook):

- **catalog**, **schema_name**: Unity Catalog catalog and schema.
- **raw_data_path**: Base path to TPC-DI data; run_tpcdi_batch appends `/sf={sf}`.
- **sf**: Scale factor (e.g. 10).
- **batch_id**: 1 for batch, 2+ for incremental.
- **load_type**: `batch` or `incremental`.

## Schema patterns

- **Bronze**: Raw ingestion; metadata `_batch_id`, `_load_timestamp`, `_source_file`.
- **Silver**: Typed, conformed; SCD Type 2 where applicable.
- **Gold**: Star schema; surrogate keys; SCD Type 2 dimensions.

See **RUN_V2.md** and **databricks/WORKFLOW_README.md** for run instructions and troubleshooting.
