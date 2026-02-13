# TPC-DI v3 (PySpark)

v3 reimplements the v2 batch pipeline in **PySpark** (no SQL files). Table and column names match **v2** exactly for compatibility.

## Structure

- **v3/dataproc/** – Dataproc runner and ETL
  - `run_tpcdi_batch.py` – Main entry (same CLI as v2: `--database`, `--raw-data-path`, `--sf`, `--batch-id`, `--load-type batch`)
  - `etl/bronze.py` – Bronze loads (v2 table/column names)
  - `etl/silver.py` – Silver transforms (v2 table/column names)
  - `etl/gold.py` – Gold loads (v2 table/column names)

## Usage

From v3/dataproc (or with `--raw-data-path` and optional `--sql-base-path`):

```bash
spark-submit run_tpcdi_batch.py \
  --database tpcdi_dw \
  --raw-data-path gs://your-bucket/tpcdi \
  --sf 10 \
  --load-type batch \
  --batch-id 1
```

Uses **v2/dataproc/tpcdi_metrics** for reporting (adds v2/dataproc to `sys.path`). Bronze customer_mgmt, finwire, and silver customers/accounts still call v2 Python scripts if present.
