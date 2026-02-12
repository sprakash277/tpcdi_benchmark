# How to Run TPC-DI v2 (run_tpcdi_batch + sql/)

This guide describes the **single-notebook** v2 flow on Databricks: **run_tpcdi_batch** plus all SQL under **sql/**.

## Prerequisites

### Databricks
- Databricks workspace with Unity Catalog enabled
- TPC-DI raw data in DBFS, Volumes, or external storage (e.g. GCS)
- Cluster or SQL Warehouse for running the notebook

---

## Databricks: run_tpcdi_batch + create_v2_workflow_notebook

### 1. Create the job (one-time)

1. Open **v2/databricks/create_v2_workflow_notebook.py** in Databricks (as a notebook).
2. Set widgets: **job_name**, **workspace_path**, **catalog**, **schema_name**, **raw_data_path**, **sf**, **load_type**, **cloud**, **num_workers**, etc.
3. Run the notebook. It creates a Databricks job with **one task**: **run_tpcdi_batch**.

### 2. Run the pipeline

- Trigger the job from the **Workflows → Jobs** UI, or run it via CLI/API.
- The job runs **run_tpcdi_batch**, which:
  - Reads SQL from **sql/bronze/**, **sql/silver/**, **sql/gold/** (and runs Python sub-notebooks in **sql/bronze/batch/** and **sql/silver/batch/** for CustomerMgmt, FinWire, customers, accounts).
  - Executes Bronze → Silver → Gold (batch or incremental based on **load_type** and **batch_id**).

### 3. What run_tpcdi_batch uses

| Layer  | Location | Contents |
|--------|----------|----------|
| Bronze | **sql/bronze/** | Load SQL (date, time, trade, etc.) + **sql/bronze/batch/** notebooks: `load_bronze_customer_mgmt`, `load_bronze_finwire` |
| Silver | **sql/silver/** | Transform SQL + **sql/silver/batch/** notebooks: `transform_silver_customers`, `transform_silver_accounts` |
| Gold   | **sql/gold/**   | Load, incremental, optimize, create_gold_dim_messages SQL |

### 4. Manual run (notebook only)

1. Open **v2/databricks/run_tpcdi_batch.py** as a notebook in Databricks.
2. Set widgets: **catalog**, **schema_name**, **raw_data_path**, **sf**, **batch_id**, **load_type** (batch or incremental).
3. Run all cells.

### 5. Variables and paths

- **raw_data_path**: Base path to TPC-DI data (e.g. `gs://bucket/tpcdi` or `dbfs:/mnt/tpcdi`). run_tpcdi_batch appends `/sf={sf}`.
- **catalog** / **schema_name**: Unity Catalog catalog and schema (e.g. schema name may include scale factor, e.g. `tpcdi_sf10`).
- **load_type**: `batch` (full load) or `incremental` (batch 2+).
- **batch_id**: Batch ID (1 for batch load; 2+ for incremental).

---

## Quick reference

```text
v2/databricks/
├── create_v2_workflow_notebook.py   # Creates job that runs run_tpcdi_batch
├── run_tpcdi_batch.py              # Single notebook: Bronze → Silver → Gold
├── tpcdi_metrics.py                # Metrics/reporting used by run_tpcdi_batch
└── sql/
    ├── bronze/                     # Bronze SQL + bronze/batch/*.py (customer_mgmt, finwire)
    ├── silver/                     # Silver SQL + silver/batch/*.py (customers, accounts)
    └── gold/                       # Gold SQL (load, incremental, optimize)
```

See also: **v2/databricks/QUICK_START.md**, **v2/databricks/WORKFLOW_README.md**.

---

## Verification

After a run, you can check:

```sql
USE CATALOG your_catalog;
USE SCHEMA your_schema;

-- Bronze
SELECT COUNT(*) FROM bronze_trade;
SELECT COUNT(*) FROM bronze_customer_mgmt;

-- Silver
SELECT COUNT(*) FROM silver_trades;
SELECT COUNT(*) FROM silver_customers;

-- Gold
SELECT COUNT(*) FROM gold_dim_customer;
SELECT COUNT(*) FROM gold_fact_trade;
SELECT * FROM gold_dim_messages ORDER BY message_timestamp DESC LIMIT 100;
```

---

## Troubleshooting

1. **Table not found**: Ensure **catalog** and **schema_name** match where tables were created; run_tpcdi_batch creates tables in that schema.
2. **Raw data not found**: Check **raw_data_path** and **sf**; path should resolve to `{raw_data_path}/sf={sf}/` with Batch1/, Batch2/, etc.
3. **Sub-notebook not found**: Ensure **sql/bronze/batch/** and **sql/silver/batch/** are in the same repo/workspace path as run_tpcdi_batch (or set **sql_base_path** if needed).
