# Running Benchmarks from Your Laptop

The `run_benchmark.py` wrapper script allows you to run TPC-DI benchmarks from your local laptop, submitting jobs to Dataproc or Databricks clusters, or running locally.

## Automatic Cluster Sizing

The script automatically configures cluster resources based on scale factor:

| Scale Factor | Worker Nodes | Instance Type (GCP) |
|--------------|--------------|---------------------|
| 10           | 2            | n2d-standard-16     |
| 100          | 3            | n2d-standard-16     |
| 1000         | 5            | n2d-standard-16     |

- **Databricks**: Automatically sets worker count and node type when creating new jobs
- **Dataproc**: Provides recommendations and auto-sets cluster metadata for metrics logging
- **GCP**: Defaults to `n2d-standard-16` for both worker and driver nodes
- Override with `--num-workers` or `--node-type-id` if needed

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

# Submit to Databricks workflow (creates job if missing)
python run_benchmark.py databricks \
  --load-type batch \
  --scale-factor 10 \
  --output-path dbfs:/mnt/tpcdi \
  --workspace-path /Workspace/Repos/user/repo/databricks \
  --cloud AWS \
  --node-type-id i3.xlarge \
  --num-workers 2

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
- **Important:** The Databricks notebooks must exist in your workspace before creating/running the workflow:
  - `generate_tpcdi_data_notebook.py` (or path specified by `--data-gen-notebook`)
  - `benchmark_databricks_notebook.py` (or path specified by `--benchmark-notebook`)
  
  See **"Notebook Upload Requirements"** section below for how to upload them.

### For Local Execution
- Spark installed locally (e.g., via `pip install pyspark`)
- All dependencies from `requirements.txt` installed
- Access to data (local filesystem or GCS with credentials)

## Platform-Specific Usage

### Dataproc

Submits a PySpark job to a Dataproc cluster using `gcloud dataproc jobs submit`. **Can automatically create the cluster if missing.**

**Automatic cluster creation:**
- Use `--create-cluster` to create cluster if it doesn't exist (uses default network)
- Use `--create-network` to create full infrastructure: VPC, subnet, firewall, and cluster
- Cluster configuration is auto-set based on scale factor:
  - SF=10 → 2 worker nodes
  - SF=100 → 3 worker nodes  
  - SF=1000 → 5 worker nodes
- For GCP, defaults to `n2d-standard-16` instance type for both worker and master nodes
- If cluster doesn't exist and `--create-cluster`/`--create-network` not provided, script exits with error

**Required arguments:**
- `--cluster`: Dataproc cluster name
- `--project-id`: GCP project ID
- `--gcs-bucket`: GCS bucket name
- `--load-type`: `batch` or `incremental`
- `--scale-factor`: TPC-DI scale factor (e.g., 10, 100, 1000)

**Example (existing cluster):**
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
  --metrics-output gs://my-bucket/tpcdi/metrics
```

**Example (auto-create cluster with default network):**
```bash
python run_benchmark.py dataproc \
  --cluster my-dataproc-cluster \
  --load-type batch \
  --scale-factor 100 \
  --gcs-bucket my-bucket \
  --project-id my-project \
  --region us-central1 \
  --create-cluster \
  --format delta
```

**Example (auto-create full infrastructure: VPC + subnet + cluster):**
```bash
python run_benchmark.py dataproc \
  --cluster my-dataproc-cluster \
  --load-type batch \
  --scale-factor 100 \
  --gcs-bucket my-bucket \
  --project-id my-project \
  --region us-central1 \
  --create-network \
  --vpc-name my-vpc \
  --subnet-name my-subnet \
  --format delta
```

**Note:** 
- Cluster metadata (instance type and worker count) is automatically set based on scale factor:
  - SF=100 → 3 workers, n2d-standard-16 instance type
- With `--create-network`, creates isolated VPC with Private Google Access (no external IPs)
- Override with `--cluster-instance-type` and `--cluster-worker-count` if your cluster differs

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

Submits a run to a Databricks workflow/job. **Automatically creates the job if it doesn't exist.**

**Required arguments:**
- `--load-type`: `batch` or `incremental`
- `--scale-factor`: TPC-DI scale factor

**Example (job auto-created if missing, GCP):**
```bash
python run_benchmark.py databricks \
  --load-type batch \
  --scale-factor 100 \
  --output-path dbfs:/mnt/tpcdi \
  --target-database tpcdi_warehouse \
  --target-schema dw \
  --metrics-output dbfs:/mnt/tpcdi/metrics \
  --workspace-path /Workspace/Repos/user/repo/databricks \
  --cloud GCP
```

**Note:** For GCP, this automatically:
- Sets node type to `n2d-standard-16` (worker and driver)
- Sets worker count to `3` (based on SF=100)
- Override with `--node-type-id` or `--num-workers` if needed

**Example (use existing job by ID):**
```bash
python run_benchmark.py databricks \
  --job-id 123 \
  --load-type batch \
  --scale-factor 100 \
  --output-path dbfs:/mnt/tpcdi
```

**Example (find existing job by name):**
```bash
python run_benchmark.py databricks \
  --job-name "TPC-DI-Benchmark" \
  --load-type batch \
  --scale-factor 100 \
  --output-path dbfs:/mnt/tpcdi
```

**Job creation arguments (used when creating new job):**
- `--job-name`: Job name (default: `TPC-DI-Benchmark`)
- `--workspace-path`: Workspace path prefix for notebooks (e.g., `/Workspace/Repos/user/repo/databricks`)
- `--data-gen-notebook`: Data generation notebook path (default: `generate_tpcdi_data_notebook`)
- `--benchmark-notebook`: Benchmark notebook path (default: `benchmark_databricks_notebook`)
- `--spark-version`: Databricks Runtime version (default: `14.3.x-scala2.12`)
- `--cloud`: Cloud provider: `AWS`, `GCP`, or `Azure` (default: `AWS`)
- `--node-type-id`: Worker node type (GCP defaults to `n2d-standard-16`, AWS defaults to `i3.xlarge`)
- `--driver-node-type-id`: Driver node type (defaults to worker type)
- `--num-workers`: Number of worker nodes (auto-set based on scale_factor if not provided: SF=10→2, SF=100→3, SF=1000→5)
- `--existing-cluster-id`: Use existing cluster instead of creating new

**Automatic cluster sizing:**
- Worker count is automatically set based on scale factor:
  - SF=10 → 2 workers
  - SF=100 → 3 workers
  - SF=1000 → 5 workers
- For GCP (`--cloud GCP`), node type defaults to `n2d-standard-16` for both worker and driver
- Override with `--num-workers` or `--node-type-id` if needed

**Runtime arguments:**
- `--output-path`: Raw data location (DBFS, Volume, or GCS path)
- `--target-catalog`: Unity Catalog name
- `--cluster-instance-type`: Worker instance type for metrics
- `--cluster-worker-count`: Number of worker instances for metrics
- `--cluster-master-type`: Driver instance type for metrics

**How it works:**
1. If `--job-id` is provided, uses that job (verifies it exists)
2. If `--job-id` is not provided, searches for job by `--job-name`
3. If job not found, creates a new job with the specified configuration
4. Submits a run to the job with the provided parameters

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

## Notebook Upload Requirements

**Important:** Before creating a Databricks workflow, the notebooks must exist in your Databricks workspace at the paths specified by `--workspace-path` and `--data-gen-notebook`/`--benchmark-notebook`.

### How Notebook Paths Work

The workflow references notebooks by their **Databricks workspace path**, not local file paths. For example:
- If `--workspace-path /Workspace/Repos/user/repo/databricks` and `--data-gen-notebook generate_tpcdi_data_notebook`
- The workflow will look for: `/Workspace/Repos/user/repo/databricks/generate_tpcdi_data_notebook`

### Upload Methods

#### Option 1: Databricks Repos (Recommended)

If you're using Databricks Repos, clone this repository:
1. In Databricks UI: **Repos** → **Add Repo**
2. Clone your Git repository
3. The notebooks will be available at `/Workspace/Repos/<org>/<repo>/databricks/`
4. Use that path as `--workspace-path`

#### Option 2: Upload via UI

1. In Databricks UI: **Workspace** → Navigate to your target folder
2. Right-click → **Import**
3. Upload `databricks/generate_tpcdi_data_notebook.py` and `databricks/benchmark_databricks_notebook.py`
4. Note the full path (e.g., `/Workspace/Users/you/databricks/generate_tpcdi_data_notebook`)
5. Use that path as `--workspace-path` or provide full paths in `--data-gen-notebook`/`--benchmark-notebook`

#### Option 3: Upload via CLI

```bash
# Upload notebooks to workspace
databricks workspace import databricks/generate_tpcdi_data_notebook.py \
  /Workspace/Users/you/databricks/generate_tpcdi_data_notebook \
  -l PYTHON

databricks workspace import databricks/benchmark_databricks_notebook.py \
  /Workspace/Users/you/databricks/benchmark_databricks_notebook \
  -l PYTHON
```

Then use `--workspace-path /Workspace/Users/you/databricks` when running the wrapper.

### Verification

The wrapper script automatically checks if notebooks exist before creating the workflow. If notebooks are missing, you'll see a warning:

```
⚠ WARNING: Some notebooks are missing in the Databricks workspace!
  ✗ 01_data_generation: /Workspace/Repos/user/repo/databricks/generate_tpcdi_data_notebook (NOT FOUND)
```

The workflow will still be created, but it will **fail when you try to run it** until the notebooks are uploaded.

## Troubleshooting

### Dataproc: "Cluster not found"
- Verify cluster name: `gcloud dataproc clusters list --region <region>`
- Check you're authenticated: `gcloud auth list`

### Databricks: "Job not found"
- If using `--job-id`, verify job ID: `databricks jobs list` or check Databricks UI
- If job doesn't exist, the script will create it automatically (provide cluster config args)
- Ensure you have permissions to create jobs in the workspace

### Databricks: "Notebook not found" or workflow fails with notebook error
- **Root cause:** The notebooks referenced in the workflow don't exist at the specified paths
- **Solution:** Upload the notebooks to Databricks workspace first (see "Notebook Upload Requirements" above)
- **Check:** The wrapper script warns you if notebooks are missing when creating the workflow
- **Verify:** Use `databricks workspace ls /Workspace/path/to/notebooks` to check if files exist

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

# Run on Databricks (creates job if missing, GCP with auto-sizing)
python run_benchmark.py databricks \
  --load-type batch \
  --scale-factor 100 \
  --output-path gs://my-bucket/tpcdi \
  --metrics-output gs://my-bucket/metrics \
  --workspace-path /Workspace/Repos/user/repo/databricks \
  --cloud GCP
# Auto-configures: 3 workers, n2d-standard-16 instance type

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

### With Cluster Metadata (Auto-set)

```bash
# Cluster metadata is auto-set based on scale factor
python run_benchmark.py dataproc \
  --cluster my-cluster \
  --load-type batch \
  --scale-factor 100 \
  --gcs-bucket my-bucket \
  --project-id my-project
# Auto-sets: cluster_instance_type=n2d-standard-16, cluster_worker_count=3

# Override if your cluster differs
python run_benchmark.py dataproc \
  --cluster my-cluster \
  --load-type batch \
  --scale-factor 100 \
  --gcs-bucket my-bucket \
  --project-id my-project \
  --cluster-instance-type n2d-standard-32 \
  --cluster-worker-count 4
```
