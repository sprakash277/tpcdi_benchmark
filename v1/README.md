# TPC-DI v1 – Python ETL and Runners

This directory contains the original TPC-DI implementation: Python ETL (bronze/silver/gold), a unified runner for Databricks/Dataproc/local, data generation, and workflow/notebook helpers.

## Layout

- **`run_benchmark.py`** – Unified wrapper: submit to Databricks, Dataproc, or run locally.
- **`run_benchmark_databricks.py`** – Entry point when the benchmark runs on Databricks (imports `benchmark` from this directory).
- **`run_benchmark_dataproc.py`** – Entry point for Dataproc jobs (use with `--py-files=benchmark.zip`).
- **`generate_tpcdi_data.py`** – CLI to generate TPC-DI raw data (uses `tools/datagen/` at project root).
- **`benchmark/`** – Python ETL package: config, runner, cost, metrics, bronze/silver/gold, platforms.
- **`databricks/`** – v1 Databricks notebooks and workflow creation (e.g. `create_databricks_workflow.py`, `generate_tpcdi_data_notebook.py`).
- **`dataproc/`** – v1 Dataproc scripts and docs.
- **`scripts/`** – Utilities (e.g. aggregate_metrics, print_customer_mgmt_schema).

## Running from project root

From the repo root (recommended so `tools/datagen/` is available):

```bash
# Data generation (DIGen in tools/datagen/)
python v1/generate_tpcdi_data.py -s 10 -o dbfs:/mnt/tpcdi

# Benchmark: Databricks
python v1/run_benchmark.py databricks --load-type batch --scale-factor 10 --target-catalog main ...

# Benchmark: Dataproc
python v1/run_benchmark.py dataproc --cluster my-cluster --load-type batch --scale-factor 10 ...

# Benchmark: local
python v1/run_benchmark.py local --load-type batch --scale-factor 10 ...
```

## Running from v1/

You can also `cd v1` and run scripts; `generate_tpcdi_data.py` will still find `tools/datagen/` at the project root.

```bash
cd v1
python run_benchmark.py databricks --load-type batch --scale-factor 10 --target-catalog main ...
python generate_tpcdi_data.py -s 10 -o dbfs:/mnt/tpcdi
```

For Dataproc, run from project root so that `v1/run_benchmark_dataproc.py` and `v1/benchmark.zip` paths are correct when submitted via gcloud.

## Databricks workflow (v1)

- Use `run_benchmark.py databricks ...` to create/run the job; it uses `databricks/create_databricks_workflow.py` and notebooks under `databricks/`.
- See `databricks/QUICK_START_WORKFLOW.md` and `databricks/WORKFLOW_README.md`.

## Prerequisites

- **tools/datagen/** at project root: DIGen.jar and pdgf/ (TPC-DI Tools v1.1.0). See main [README.md](../README.md).
- For Databricks: `databricks-cli` and credentials (or env vars).
- For Dataproc: `gcloud` and a cluster (or use `--create-cluster`).
