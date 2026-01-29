# TPC-DI Benchmark: Databricks vs Dataproc

This directory contains a benchmarking framework to compare ETL performance between **Databricks** and **Dataproc** using the TPC-DI benchmark.

## Overview

The benchmark framework:
- Supports both **batch** and **incremental** ETL loads
- Works with **Databricks** (DBFS storage) and **Dataproc** (GCS storage)
- Collects detailed performance metrics
- Provides platform-agnostic ETL transformations

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
2. **TPC-DI raw data must already exist in GCS** — `run_benchmark_dataproc.py` does **not** generate data (unlike the Databricks workflow, which can run data generation then benchmark). Generate data separately (e.g. TPC-DI DIGen, then upload to GCS).
3. Data path: `gs://<bucket>/tpcdi/sf=<scale_factor>/`
4. GCP project ID and region
5. **Metastore (optional):** If you do **not** attach a [Dataproc Metastore](https://cloud.google.com/dataproc-metastore/docs), Spark uses the default (embedded/local) metastore. The benchmark sets the warehouse to GCS (`gs://<bucket>/spark-warehouse`) and uses **two-part** table names (`database.table`, e.g. `tpcdi_warehouse_dw.bronze_date`) and **Parquet** by default (no Delta package). See **docs/DATAPROC_METASTORE.md**.

## Usage

### Databricks

#### Option 1: Databricks Notebook
1. Open `benchmark_databricks_notebook.py` in Databricks
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

### Dataproc

**Important:** The Dataproc run does **not** create or generate TPC-DI data. Ensure raw data already exists at `gs://<bucket>/tpcdi/sf=<scale_factor>/` (or your `--raw-data-path`). On Databricks, the workflow can run data generation then benchmark in one job; on Dataproc you must generate/upload data separately before submitting the benchmark.

**→ [How to run Dataproc](dataproc.md)** — full parameter list, mandatory vs optional, what each means, and a sample run with service account.

**Package the benchmark module:** When submitting via `gcloud dataproc jobs submit pyspark`, only the main script is uploaded by default. The `benchmark` package must be provided with `--py-files`. From the **project root**:

```bash
zip -r benchmark.zip benchmark
```

Then pass `--py-files=benchmark.zip` (or `--py-files=gs://<bucket>/benchmark.zip` if you upload the zip to GCS) to every `gcloud dataproc jobs submit pyspark` command below.

**Benchmark metrics:** By default, metrics are saved to GCS. Use `--no-save-metrics` to skip saving; use `--metrics-output=gs://bucket/path/metrics` to set the output path (default: `gs://<gcs-bucket>/tpcdi/metrics`).

**Table format:** Use `--format delta` or `--format parquet` (default: parquet). With `--format delta`, the benchmark adds the Delta package automatically; no extra `--packages` needed.

**Spark packages:** The benchmark adds `spark-xml` (for CustomerMgmt.xml) and, when `--format delta`, `io.delta:delta-spark_2.12` automatically. The driver must have Maven access to resolve them. For air-gapped or no-Maven setups, use the pre-bundled JAR: `--jars=libs/spark-xml_2.12-0.18.0.jar` (see **libs/README.md**).

#### Submit as Spark Job
```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --py-files=benchmark.zip \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1 \
  --target-database tpcdi_warehouse \
  --target-schema dw
```

Optional: add `--log-detailed-stats` (before or after other args) to log per-table timing and records:
```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --py-files=benchmark.zip \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1 \
  --target-database tpcdi_warehouse \
  --target-schema dw \
  --log-detailed-stats
```

For incremental:
```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --py-files=benchmark.zip \
  -- \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1
```

Add `--log-detailed-stats` after the `--` to log per-table timing and records; without it, only job start time, end time, and total duration are logged (for performance comparison). See the optional example above.

#### Running with a Service Account (SA) and Key File

When the cluster’s default identity should not be used for GCS (e.g. different project, stricter IAM), you can run the benchmark using a **service account (SA)** and its **JSON key file**. See **[dataproc.md](dataproc.md)** for a complete example (all parameters + SA).

**1. Create a service account and key (GCP Console or gcloud)**

- Create a SA in your project (e.g. `tpcdi-dataproc@<project>.iam.gserviceaccount.com`).
- Grant the SA roles that can read from your GCS bucket and write to the bucket (e.g. **Storage Object Viewer** on the raw data path, **Storage Object Admin** or **Creator** where you write tables/metrics).
- Create a JSON key for the SA and download it:
  ```bash
  gcloud iam service-accounts keys create sa-key.json \
    --iam-account=tpcdi-dataproc@<project>.iam.gserviceaccount.com
  ```

**2. Pass SA and key to the benchmark**

- **Service account email:** `--service-account-email <sa-email>`
- **Key file path:** `--service-account-key-file <path-to-json>`

Use **both** for key-file auth; the driver will use this SA for GCS (Hadoop `fs.gs`).

**Example: run locally (driver on your machine)**

```bash
python run_benchmark_dataproc.py \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1 \
  --service-account-email=tpcdi-dataproc@<project>.iam.gserviceaccount.com \
  --service-account-key-file=./sa-key.json
```

**Example: submit as a Dataproc job (key file on cluster)**

The key file path must be a path that the **driver node** can read. Options:

- **A. Key file in GCS**  
  Upload the key to a GCS path only the job can access (e.g. `gs://<bucket>/secrets/tpcdi-sa-key.json`). Ensure the cluster’s default SA can read that object. Then pass the GCS path:
  ```bash
  gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
    --cluster=<cluster-name> \
    --region=us-central1 \
    --py-files=benchmark.zip \
    -- \
    --load-type batch \
    --scale-factor 10 \
    --gcs-bucket=<your-bucket> \
    --project-id=<your-project> \
    --region=us-central1 \
    --service-account-email=tpcdi-dataproc@<project>.iam.gserviceaccount.com \
    --service-account-key-file=gs://<bucket>/secrets/tpcdi-sa-key.json
  ```

- **B. Key file on local disk**  
  Copy the key to the cluster (e.g. via init action or a one-time copy to a fixed path). Use the same `gcloud dataproc jobs submit pyspark` command as in A above with `--py-files=benchmark.zip`, and pass the local path after `--`:
  ```bash
  --service-account-key-file=/var/lib/tpcdi/sa-key.json
  ```

**3. Using only the cluster’s service account**

If the cluster is already created with the SA you want (e.g. `gcloud dataproc clusters create ... --service-account=...`), you do **not** need to pass `--service-account-email` or `--service-account-key-file`. The job will use the cluster’s default identity for GCS.

**4. Security**

- Do **not** commit the JSON key to source control. Add `*.json` (or the key filename) to `.gitignore` if the key is in the repo tree.
- Prefer storing the key in **Secret Manager** or a restricted GCS path and making it available only to the job (e.g. copy to a local path in an init script that only the job user can read).
- Prefer short-lived keys and rotating them; avoid long-lived keys in shared locations.

For more detail, see **docs/DATAPROC_SERVICE_ACCOUNT.md**.

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
- Ensure GCS connector is installed on cluster
- Verify GCS bucket permissions
- Check that raw data is accessible from cluster
- Verify project ID and region are correct
- **Tables/database gone after job or cluster ends:** Without a Dataproc Metastore (and GCS warehouse), Spark uses the default metastore and warehouse; metadata and often data are ephemeral. See **docs/DATAPROC_METASTORE.md**.

## References

- [TPC-DI Specification](https://www.tpc.org/tpcdi/)
- [Databricks TPC-DI Implementation](https://github.com/shannon-barrow/databricks-tpc-di)
- [TPC-DI Tools](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY)
