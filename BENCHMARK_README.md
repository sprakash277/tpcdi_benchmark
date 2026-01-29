# TPC-DI Benchmark: Databricks vs Dataproc

This directory contains a benchmarking framework to compare ETL performance between **Databricks** and **Dataproc** using the TPC-DI benchmark.

## Overview

The benchmark framework:
- Supports both **batch** and **incremental** ETL loads
- Works with **Databricks** (DBFS storage) and **Dataproc** (GCS storage)
- **Databricks:** Run via notebook, Python script, or **workflow** (job that generates data then runs the benchmark)
- Collects detailed performance metrics
- Provides platform-agnostic ETL transformations

## Project layout

| Path | Purpose |
|------|--------|
| **`benchmark/`** | Shared ETL, metrics, runner; platform adapters in `benchmark/platforms/` |
| **`databricks/`** | Databricks-only: notebooks (benchmark, data gen, workflow creation), workflow script, workflow docs |
| **`dataproc/`** | Dataproc-only: run script, run guide (`DataprocRun.md`), bundled JARs (`libs/`) |
| **`docs/`** | Architecture and platform-specific docs (e.g. `DATAPROC_*`) |
| **`tools/`** | TPC-DI tools (DIGen, pdgf) for data generation |
| **Root** | `generate_tpcdi_data.py`, `run_benchmark_databricks.py`, `requirements.txt`, READMEs |

**Databricks:** `databricks/benchmark_databricks_notebook`, `databricks/generate_tpcdi_data_notebook`, `databricks/create_workflow_notebook`, `databricks/create_databricks_workflow`, `databricks/WORKFLOW_README`, `databricks/QUICK_START_WORKFLOW`.  
**Dataproc:** `dataproc/run_benchmark_dataproc`, `dataproc/DataprocRun.md`, `dataproc/libs/`.

## Architecture

```
benchmark/
├── config.py          # Configuration management
├── metrics.py          # Performance metrics collection
├── runner.py           # Main benchmark orchestrator
├── platforms/          # Platform adapters
│   ├── databricks.py  # Databricks/DBFS adapter
│   └── dataproc.py    # Dataproc/GCS adapter
└── etl/               # ETL transformations
    ├── batch.py       # Batch load logic
    └── incremental.py # Incremental load logic
```

## Prerequisites

### Databricks
1. Databricks workspace with cluster
2. TPC-DI raw data (generated using `generate_tpcdi_data.py`)
3. **Output path** = raw data location (benchmark input):
   - **DBFS**: `dbfs:/mnt/tpcdi` → data at `dbfs:/mnt/tpcdi/sf=<scale_factor>/`
   - **Volume**: `/Volumes/<catalog>/<schema>/<volume>` → data at `.../volume/sf=<scale_factor>/`

### Dataproc
1. Dataproc cluster with GCS connector installed
2. **TPC-DI raw data must already exist in GCS** — `dataproc/run_benchmark_dataproc.py` does **not** generate data (unlike the Databricks workflow). Generate data separately (e.g. TPC-DI DIGen, then upload to GCS).
3. Data path: `gs://<bucket>/tpcdi/sf=<scale_factor>/`
4. GCP project ID and region
5. **Metastore (optional):** If you do **not** attach a [Dataproc Metastore](https://cloud.google.com/dataproc-metastore/docs), Spark uses the default metastore. See **docs/DATAPROC_METASTORE.md**.
6. **How to run:** All Dataproc run commands, parameters, and setup → **[dataproc/DataprocRun.md](dataproc/DataprocRun.md)**.

## Usage

### Databricks

#### Option 1: Databricks Notebook
1. Open `databricks/benchmark_databricks_notebook.py` in Databricks
2. Configure widgets:
   - **Load Type**: batch or incremental
   - **Scale Factor**: e.g., 10, 100, 1000
   - **Output Path**: Raw data location (DBFS or Volume base, e.g. `dbfs:/mnt/tpcdi`)
   - **Use Volume**: `true` if raw data is in a Unity Catalog Volume
   - **Target Database/Schema**: Where to write results
   - **Batch ID**: Required for incremental loads
   - **Metrics Output**: Where to save metrics JSON
3. Run all cells

#### Option 2: Python Script
```bash
# DBFS
python run_benchmark_databricks.py \
  --load-type batch \
  --scale-factor 10 \
  --output-path dbfs:/mnt/tpcdi \
  --target-database tpcdi_warehouse \
  --target-schema dw \
  --metrics-output dbfs:/mnt/tpcdi/metrics

# Unity Catalog Volume
python run_benchmark_databricks.py \
  --load-type batch \
  --scale-factor 10 \
  --output-path /Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume \
  --use-volume \
  --target-database tpcdi_warehouse \
  --target-schema dw
```

For incremental:
```bash
python run_benchmark_databricks.py \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --output-path dbfs:/mnt/tpcdi \
  --target-database tpcdi_warehouse \
  --target-schema dw
```

#### Option 3: Databricks Workflow

Run the benchmark as a **Databricks workflow (job)** that (1) generates TPC-DI raw data and (2) runs the benchmark ETL in sequence. All config is via workflow parameters.

**Step 1: Generate the workflow from the notebook**

1. **Open the workflow-creation notebook in Databricks**  
   Import or open `databricks/create_workflow_notebook.py` in your workspace (e.g. **Repos** or **Workspace**).

2. **Set Job Name** (widget: **Job Name**)  
   Default: `TPC-DI-Benchmark`. Change if you want a different job name.

3. **Set notebook paths** (optional)  
   - **Data Generation Notebook Path**: Notebook that generates TPC-DI data. Default: `generate_tpcdi_data_notebook` (in `databricks/`). Use just the name if it lives next to the workflow notebook, or a full path (e.g. `/Workspace/Repos/user/repo/databricks/generate_tpcdi_data_notebook`).  
   - **Benchmark Notebook Path**: Notebook that runs the benchmark. Default: `benchmark_databricks_notebook` (in `databricks/`). Same rules as above.

4. **Set cluster config**  
   - **Cluster Spark Version (DBR)**: Choose a DBR version (e.g. `14.3.x-scala2.12`, `15.4.x-photon-scala2.12`) from the dropdown.  
   - **Cloud**: `AWS`, `GCP`, or `Azure` (sets default node types when you leave Worker/Driver blank).  
   - **Worker / Driver node types**: Leave blank to use cloud defaults, or set explicitly (e.g. `i3.xlarge` on AWS). On **GCP**, use the **GCP Worker Node Type** and **GCP Driver Node Type** dropdowns (e.g. `n2d-standard-16`).  
   - **Number of Workers**: e.g. `2` (default).

5. **Use an existing cluster (optional)**  
   If you have a cluster to reuse, set **Existing Cluster ID** to its ID. Otherwise the workflow creates new clusters per run.

6. **Run all cells**  
   Execute the notebook from top to bottom. The last cell calls the Databricks Jobs API and creates the workflow.

7. **Copy the Job ID**  
   The final cell prints something like `Job ID: 123456789` and a link to the job. Save the **Job ID**; you use it to run the workflow.

**Step 2: Run the workflow**

- **UI:** Go to **Workflows** → **Jobs** → your job (e.g. **TPC-DI-Benchmark**) → **Run now**. Override parameters if needed (e.g. `scale_factor`, `tpcdi_raw_data_path`, `load_type`, `batch_id` for incremental) → **Run**.  
- **CLI:** `databricks jobs run-now --job-id <job-id> --notebook-params '{"scale_factor":"10","load_type":"batch"}'`  
- **API:** `POST /api/2.1/jobs/run-now` with `job_id` and `notebook_params`.

**Workflow parameters (defaults)**  
`scale_factor` (10), `tpcdi_raw_data_path` (`dbfs:/mnt/tpcdi`), `load_type` (batch), `target_database`, `target_schema`, `batch_id` (for incremental), `metrics_output`, etc. Override when you run the job.

**More detail**  
- **[databricks/QUICK_START_WORKFLOW.md](databricks/QUICK_START_WORKFLOW.md)** — quick create + run.  
- **[databricks/WORKFLOW_README.md](databricks/WORKFLOW_README.md)** — full workflow docs, parameters, CLI/API, troubleshooting.

### Dataproc

**→ [How to run Dataproc](dataproc/DataprocRun.md)** — prerequisites, all parameters (mandatory vs optional, what each means), setup, run commands (batch, incremental, `--log-detailed-stats`), running with a service account, full example with SA, and troubleshooting. All Dataproc run commands and setup live there; the run does **not** generate TPC-DI data (raw data must exist in GCS).

## Load Types

### Batch Load
Processes historical data and initial batch:
- Loads dimension tables from `HistoricalLoad/` directory
- Loads fact and dimension tables from `Batch1/`
- Creates target database and schema
- Suitable for initial data warehouse setup

### Incremental Load
Processes a specific incremental batch:
- Updates dimension tables (SCD Type 2)
- Loads new fact records
- Requires `batch_id` parameter
- Processes files from `Batch<batch_id>/` directory

## Performance Metrics

The benchmark collects detailed metrics:

- **Step-level metrics**: Duration, rows processed, bytes processed per step
- **Summary metrics**: Total duration, throughput (rows/sec, MB/sec)
- **Table metrics**: Row counts and sizes for each table
- **Error tracking**: Failed steps with error messages

Metrics are saved as JSON files with timestamps:
- Databricks: `dbfs:/mnt/tpcdi/metrics/metrics_databricks_batch_sf10_20240126_143022.json`
- Dataproc: `gs://bucket/tpcdi/metrics/metrics_dataproc_batch_sf10_20240126_143022.json`

## Comparing Results

To compare Databricks vs Dataproc:

1. Run benchmark on both platforms with the same scale factor
2. Collect metrics JSON files
3. Compare key metrics:
   - Total duration
   - Throughput (rows/sec, MB/sec)
   - Per-step durations
   - Resource utilization (if available)

Example comparison script:
```python
import json

# Load metrics
with open("metrics_databricks_batch_sf10_*.json") as f:
    db_metrics = json.load(f)
with open("metrics_dataproc_batch_sf10_*.json") as f:
    dp_metrics = json.load(f)

print(f"Databricks: {db_metrics['total_duration_seconds']:.2f}s")
print(f"Dataproc: {dp_metrics['total_duration_seconds']:.2f}s")
print(f"Speedup: {dp_metrics['total_duration_seconds'] / db_metrics['total_duration_seconds']:.2f}x")
```

## ETL Transformations

The benchmark implements core TPC-DI transformations:

### Dimension Tables (Historical)
- `DimDate` - Date dimension
- `DimTime` - Time dimension
- `DimTradeType` - Trade type lookup
- `DimStatusType` - Status type lookup
- `DimTaxRate` - Tax rate lookup
- `DimIndustry` - Industry classification

### Dimension Tables (Batch/Incremental)
- `DimAccount` - Customer accounts (SCD Type 2)
- `DimCustomer` - Customer information (SCD Type 2)

### Fact Tables
- `FactTrade` - Trade transactions

**Note**: This is a simplified implementation. The full TPC-DI specification includes more tables and complex transformations. Extend the ETL modules as needed.

## Extending the Benchmark

### Add New ETL Transformations

1. Add methods to `benchmark/etl/batch.py` or `benchmark/etl/incremental.py`
2. Call from `run_full_batch_load()` or `process_batch()`
3. Use platform adapter methods: `read_raw_file()`, `write_table()`

### Add New Metrics

1. Extend `StepMetrics` in `benchmark/metrics.py`
2. Add metadata to step finish calls
3. Update summary calculations if needed

### Support New Platforms

1. Create new adapter in `benchmark/platforms/`
2. Implement required methods (matching `DatabricksPlatform` interface)
3. Add platform enum to `config.py`
4. Update `runner.py` to handle new platform

## Troubleshooting

### Databricks
- Ensure raw data exists at specified DBFS path
- Check cluster has sufficient resources for scale factor
- Verify SparkSession is available

### Dataproc
- See **[dataproc/DataprocRun.md](dataproc/DataprocRun.md)** for run commands, SA setup, and Dataproc-specific troubleshooting.

## References

- [TPC-DI Specification](https://www.tpc.org/tpcdi/)
- [Databricks TPC-DI Implementation](https://github.com/shannon-barrow/databricks-tpc-di)
- [TPC-DI Tools](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY)
