# Running Benchmarks from Your Laptop

The `run_benchmark.py` wrapper script allows you to run TPC-DI benchmarks from your local laptop, submitting jobs to Dataproc or Databricks clusters, or running locally.

## Quick Start

```bash
# Submit to Dataproc
python run_benchmark.py dataproc \
  --cluster my-cluster \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket my-bucket \
  --project-id my-project \
  --region us-central1

# Submit to Databricks workflow
python run_benchmark.py databricks \
  --job-id 123 \
  --load-type batch \
  --scale-factor 10 \
  --output-path dbfs:/mnt/tpcdi

# Run locally (requires Spark installed)
python run_benchmark.py local \
  --load-type batch \
  --scale-factor 10 \
  --raw-data-path ./data \
  --metrics-output ./metrics
```

## Prerequisites

### For Dataproc Submission
- `gcloud` CLI installed and authenticated (`gcloud auth login`)
- Access to the Dataproc cluster
- `benchmark.zip` will be created automatically if missing

### For Databricks Submission
- Option A: `databricks-cli` installed (`pip install databricks-cli`) and configured (`databricks configure --token`)
- Option B: Set `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables

### For Local Execution
- Spark installed locally (e.g., via `pip install pyspark`)
- All dependencies from `requirements.txt` installed
- Access to data (local filesystem or GCS with credentials)

## Platform-Specific Usage

### Dataproc

Submits a PySpark job to a Dataproc cluster using `gcloud dataproc jobs submit`.

**Required arguments:**
- `--cluster`: Dataproc cluster name
- `--project-id`: GCP project ID
- `--gcs-bucket`: GCS bucket name
- `--load-type`: `batch` or `incremental`
- `--scale-factor`: TPC-DI scale factor (e.g., 10, 100, 1000)

**Example:**
```bash
python run_benchmark.py dataproc \
  --cluster my-dataproc-cluster \
  --load-type batch \
  --scale-factor 100 \
  --gcs-bucket my-bucket \
  --project-id my-project \
  --region us-central1 \
  --raw-data-path gs://my-bucket/tpcdi \
  --format delta \
  --metrics-output gs://my-bucket/tpcdi/metrics \
  --cluster-instance-type n2d-standard-16 \
  --cluster-worker-count 4
```

**Optional arguments:**
- `--raw-data-path`: Base path to raw TPC-DI data (default: `gs://<bucket>/tpcdi`)
- `--format`: Table format: `delta` or `parquet` (default: `parquet`)
- `--service-account-email`: Service account for GCS access
- `--service-account-key-file`: Path to service account JSON key file
- `--jars`: Additional JAR files (comma-separated)
- `--cluster-instance-type`: Worker instance type for metrics
- `--cluster-worker-count`: Number of worker instances for metrics
- `--cluster-master-type`: Driver instance type for metrics

### Databricks

Submits a run to an existing Databricks workflow/job.

**Required arguments:**
- `--job-id`: Databricks job/workflow ID
- `--load-type`: `batch` or `incremental`
- `--scale-factor`: TPC-DI scale factor

**Example:**
```bash
python run_benchmark.py databricks \
  --job-id 123 \
  --load-type batch \
  --scale-factor 100 \
  --output-path dbfs:/mnt/tpcdi \
  --target-database tpcdi_warehouse \
  --target-schema dw \
  --metrics-output dbfs:/mnt/tpcdi/metrics
```

**Optional arguments:**
- `--output-path`: Raw data location (DBFS, Volume, or GCS path)
- `--target-catalog`: Unity Catalog name
- `--cluster-instance-type`: Worker instance type for metrics
- `--cluster-worker-count`: Number of worker instances for metrics
- `--cluster-master-type`: Driver instance type for metrics

**Note:** The workflow must already exist. Create it using:
- `databricks/create_workflow_notebook.py` (in Databricks UI)
- `databricks/create_databricks_workflow.py` (from command line)

### Local Execution

Runs the benchmark locally using your local Spark installation.

**Required arguments:**
- `--load-type`: `batch` or `incremental`
- `--scale-factor`: TPC-DI scale factor

**Example (local filesystem):**
```bash
python run_benchmark.py local \
  --load-type batch \
  --scale-factor 10 \
  --raw-data-path ./data/tpcdi \
  --metrics-output ./metrics
```

**Example (GCS data):**
```bash
python run_benchmark.py local \
  --load-type batch \
  --scale-factor 10 \
  --raw-data-path gs://my-bucket/tpcdi \
  --gcs-bucket my-bucket \
  --project-id my-project \
  --service-account-key-file ./key.json \
  --metrics-output ./metrics
```

**Platform detection:** The script detects platform from the data path:
- `gs://` paths → Dataproc platform
- Other paths → Databricks platform

## Common Arguments

All platforms support these common arguments:

- `--load-type`: `batch` or `incremental` (required)
- `--scale-factor`: TPC-DI scale factor (required)
- `--target-database`: Target database name (default: `tpcdi_warehouse`)
- `--target-schema`: Target schema name (default: `dw`)
- `--batch-id`: Batch ID for incremental loads
- `--metrics-output`: Path to save metrics JSON
- `--log-detailed-stats`: Enable per-table timing and record counts
- `--cluster-instance-type`: Worker instance type for metrics
- `--cluster-worker-count`: Number of worker instances for metrics
- `--cluster-master-type`: Driver/master instance type for metrics

## Automatic Packaging

The script automatically creates `benchmark.zip` if it doesn't exist. This package is uploaded to Dataproc clusters or used for local runs.

To manually create it:
```bash
zip -r benchmark.zip benchmark
```

## Environment Variables

### Databricks (when CLI not available)
- `DATABRICKS_HOST`: Databricks workspace URL (e.g., `https://workspace.cloud.databricks.com`)
- `DATABRICKS_TOKEN`: Personal access token or OAuth token

### GCP (for local runs with GCS)
- `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account JSON key file
- `GOOGLE_CLOUD_PROJECT`: GCP project ID (used if `--project-id` not provided)

## Troubleshooting

### Dataproc: "Cluster not found"
- Verify cluster name: `gcloud dataproc clusters list --region <region>`
- Check you're authenticated: `gcloud auth list`

### Databricks: "Job not found"
- Verify job ID: `databricks jobs list` or check Databricks UI
- Ensure you have access to the job

### Local: "Cannot import benchmark"
- Run from project root directory
- Install dependencies: `pip install -r requirements.txt`
- Ensure `benchmark/` directory exists

### Local: "Spark not found"
- Install PySpark: `pip install pyspark`
- Or set `SPARK_HOME` environment variable

## Examples

### Compare Dataproc vs Databricks

```bash
# Run on Dataproc
python run_benchmark.py dataproc \
  --cluster dataproc-cluster \
  --load-type batch \
  --scale-factor 100 \
  --gcs-bucket my-bucket \
  --project-id my-project \
  --metrics-output gs://my-bucket/metrics

# Run on Databricks
python run_benchmark.py databricks \
  --job-id 123 \
  --load-type batch \
  --scale-factor 100 \
  --output-path gs://my-bucket/tpcdi \
  --metrics-output gs://my-bucket/metrics

# Aggregate results
python scripts/aggregate_metrics.py \
  --input ./metrics \
  --output ./comparison.csv
```

### Incremental Load

```bash
python run_benchmark.py dataproc \
  --cluster my-cluster \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --gcs-bucket my-bucket \
  --project-id my-project
```

### With Cluster Metadata

```bash
python run_benchmark.py dataproc \
  --cluster my-cluster \
  --load-type batch \
  --scale-factor 100 \
  --gcs-bucket my-bucket \
  --project-id my-project \
  --cluster-instance-type n2d-standard-16 \
  --cluster-worker-count 4 \
  --cluster-master-type n2d-standard-16
```
