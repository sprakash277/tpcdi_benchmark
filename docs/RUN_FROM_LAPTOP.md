# Running Benchmarks from Your Laptop

The `run_benchmark.py` wrapper script allows you to run TPC-DI benchmarks from your local laptop, submitting jobs to Dataproc or Databricks clusters, or running locally.

## Quick Reference

### Required Arguments (All Platforms)
- `--load-type`: `batch` or `incremental`
- `--scale-factor`: TPC-DI scale factor (e.g., 10, 100, 1000)

### Platform-Specific Required Arguments

**Dataproc:**
- `--cluster`: Dataproc cluster name
- `--project-id`: GCP project ID
- `--gcs-bucket`: GCS bucket name

**Databricks:**
- No additional required arguments (job is auto-created if missing)

**Local:**
- No additional required arguments (platform auto-detected from data path)

### Automatic Cluster Sizing

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

**See "Complete Parameter Reference" section below for all optional parameters.**

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

**All Optional Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--raw-data-path` | `gs://<bucket>/tpcdi` | Base path to raw TPC-DI data in GCS |
| `--format` | `parquet` | Table format: `delta` or `parquet` |
| `--region` | `us-central1` | GCP region |
| `--spark-master` | `yarn` | Spark master URL |
| `--service-account-email` | - | Service account email for GCS access |
| `--service-account-key-file` | - | Path to service account JSON key file (local path, not gs://) |
| `--jars` | - | Additional JAR files (comma-separated) |
| `--target-database` | `tpcdi_warehouse` | Target database name |
| `--target-schema` | `dw` | Target schema name |
| `--batch-id` | - | Batch ID for incremental loads |
| `--metrics-output` | `gs://<bucket>/tpcdi/metrics` | Path to save metrics JSON |
| `--log-detailed-stats` | `false` | Enable per-table timing and record counts |
| `--cluster-instance-type` | Auto-detected | Worker instance type for metrics logging |
| `--cluster-worker-count` | Auto-detected | Number of worker instances for metrics logging |
| `--cluster-master-type` | Auto-detected | Driver/master instance type for metrics logging |
| `--create-cluster` | `false` | Create cluster if it doesn't exist (uses default network) |
| `--create-network` | `false` | Create VPC, subnet, firewall, and cluster if missing |
| `--vpc-name` | `<cluster>-vpc` | VPC name (used with `--create-network`) |
| `--subnet-name` | `<cluster>-subnet` | Subnet name (used with `--create-network` or `--create-cluster`) |
| `--subnet-range` | `10.10.0.0/24` | Subnet CIDR range (used with `--create-network`) |
| `--zone` | `<region>-b` | GCP zone |
| `--firewall-rule-name` | `allow-<subnet>-internal` | Firewall rule name (used with `--create-network`) |

**Note:** See "Complete Parameter Reference" section below for a comprehensive table of all parameters.

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

**All Optional Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--job-id` | - | Databricks job/workflow ID (if not provided, searches by `--job-name` or creates new) |
| `--job-name` | `TPC-DI-Benchmark` | Job name (used to find existing job or name new job) |
| `--output-path` | - | Raw data location: DBFS, Volume, or GCS path |
| `--target-database` | `tpcdi_warehouse` | Target database name |
| `--target-schema` | `dw` | Target schema name |
| `--target-catalog` | - | Unity Catalog name (optional) |
| `--batch-id` | - | Batch ID for incremental loads |
| `--metrics-output` | `dbfs:/mnt/tpcdi/metrics` | Path to save metrics JSON |
| `--log-detailed-stats` | `false` | Enable per-table timing and record counts |
| `--workspace-path` | - | Workspace path prefix for notebooks (e.g., `/Workspace/Repos/user/repo/databricks`) |
| `--data-gen-notebook` | `generate_tpcdi_data_notebook` | Data generation notebook path (relative to `--workspace-path`) |
| `--benchmark-notebook` | `benchmark_databricks_notebook` | Benchmark notebook path (relative to `--workspace-path`) |
| `--spark-version` | `14.3.x-scala2.12` | Databricks Runtime version (for new jobs) |
| `--cloud` | `AWS` | Cloud provider: `AWS`, `GCP`, or `Azure` (for new jobs) |
| `--node-type-id` | Auto (GCP: `n2d-standard-16`, AWS: `i3.xlarge`) | Worker node type (for new jobs) |
| `--driver-node-type-id` | Same as `--node-type-id` | Driver node type (for new jobs) |
| `--num-workers` | Auto (SF=10→2, SF=100→3, SF=1000→5) | Number of worker nodes (for new jobs) |
| `--existing-cluster-id` | - | Use existing cluster ID instead of creating new (for new jobs) |
| `--cluster-instance-type` | Auto-detected | Worker instance type for metrics logging |
| `--cluster-worker-count` | Auto-detected | Number of worker instances for metrics logging |
| `--cluster-master-type` | Auto-detected | Driver instance type for metrics logging |

**Automatic cluster sizing:**
- Worker count is automatically set based on scale factor:
  - SF=10 → 2 workers
  - SF=100 → 3 workers
  - SF=1000 → 5 workers
- For GCP (`--cloud GCP`), node type defaults to `n2d-standard-16` for both worker and driver
- Override with `--num-workers` or `--node-type-id` if needed

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

**All Optional Arguments:**

| Argument | Default | Description |
|----------|---------|-------------|
| `--raw-data-path` | `.` | Path to raw TPC-DI data (local or `gs://`) |
| `--output-path` | - | Output path (for Databricks platform) |
| `--target-database` | `tpcdi_warehouse` | Target database name |
| `--target-schema` | `dw` | Target schema name |
| `--target-catalog` | - | Unity Catalog name (for Databricks platform) |
| `--batch-id` | - | Batch ID for incremental loads |
| `--metrics-output` | `./metrics` | Path to save metrics JSON |
| `--log-detailed-stats` | `false` | Enable per-table timing and record counts |
| `--gcs-bucket` | - | GCS bucket (required if `--raw-data-path` is `gs://`) |
| `--project-id` | `GOOGLE_CLOUD_PROJECT` env var | GCP project ID (required for Dataproc platform) |
| `--region` | `us-central1` | GCP region |
| `--service-account-email` | - | Service account email for GCS |
| `--service-account-key-file` | - | Path to service account JSON key file |
| `--format` | `parquet` | Table format: `delta` or `parquet` |
| `--cluster-instance-type` | Auto-detected | Worker instance type for metrics logging |
| `--cluster-worker-count` | Auto-detected | Number of worker instances for metrics logging |
| `--cluster-master-type` | Auto-detected | Driver/master instance type for metrics logging |

**Platform detection:** The script detects platform from the data path:
- `gs://` paths → Dataproc platform
- Other paths → Databricks platform

## Complete Parameter Reference

### Common Arguments (All Platforms)

These arguments are available for all platforms (`dataproc`, `databricks`, `local`):

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--load-type` | ✅ Yes | - | Type of load: `batch` or `incremental` |
| `--scale-factor` | ✅ Yes | - | TPC-DI scale factor (e.g., 10, 100, 1000) |
| `--target-database` | ❌ No | `tpcdi_warehouse` | Target database name |
| `--target-schema` | ❌ No | `dw` | Target schema name |
| `--batch-id` | ❌ No | - | Batch ID for incremental loads (required if `--load-type incremental`) |
| `--metrics-output` | ❌ No | Platform-specific | Path to save metrics JSON |
| `--log-detailed-stats` | ❌ No | `false` | Enable per-table timing and record counts |
| `--cluster-instance-type` | ❌ No | Auto-detected | Worker instance type for metrics logging |
| `--cluster-worker-count` | ❌ No | Auto-detected | Number of worker instances for metrics logging |
| `--cluster-master-type` | ❌ No | Auto-detected | Driver/master instance type for metrics logging |

### Dataproc-Specific Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--cluster` | ✅ Yes | - | Dataproc cluster name |
| `--project-id` | ✅ Yes | - | GCP project ID |
| `--gcs-bucket` | ✅ Yes | - | GCS bucket name |
| `--region` | ❌ No | `us-central1` | GCP region |
| `--raw-data-path` | ❌ No | `gs://<bucket>/tpcdi` | Base path to raw TPC-DI data in GCS |
| `--format` | ❌ No | `parquet` | Table format: `delta` or `parquet` |
| `--spark-master` | ❌ No | `yarn` | Spark master URL |
| `--service-account-email` | ❌ No | - | Service account email for GCS access |
| `--service-account-key-file` | ❌ No | - | Path to service account JSON key file (local path, not gs://) |
| `--jars` | ❌ No | - | Additional JAR files (comma-separated) |
| `--create-cluster` | ❌ No | `false` | Create cluster if it doesn't exist (uses default network) |
| `--create-network` | ❌ No | `false` | Create VPC, subnet, firewall, and cluster if missing |
| `--vpc-name` | ❌ No | `<cluster>-vpc` | VPC name (used with `--create-network`) |
| `--subnet-name` | ❌ No | `<cluster>-subnet` | Subnet name (used with `--create-network` or `--create-cluster`) |
| `--subnet-range` | ❌ No | `10.10.0.0/24` | Subnet CIDR range (used with `--create-network`) |
| `--zone` | ❌ No | `<region>-b` | GCP zone |
| `--firewall-rule-name` | ❌ No | `allow-<subnet>-internal` | Firewall rule name (used with `--create-network`) |

### Databricks-Specific Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--job-id` | ❌ No | - | Databricks job/workflow ID (if not provided, searches by `--job-name` or creates new) |
| `--job-name` | ❌ No | `TPC-DI-Benchmark` | Job name (used to find existing job or name new job) |
| `--output-path` | ❌ No | - | Raw data location: DBFS, Volume, or GCS path |
| `--target-database` | ❌ No | `tpcdi_warehouse` | Target database name |
| `--target-schema` | ❌ No | `dw` | Target schema name |
| `--target-catalog` | ❌ No | - | Unity Catalog name (optional) |
| `--batch-id` | ❌ No | - | Batch ID for incremental loads |
| `--metrics-output` | ❌ No | `dbfs:/mnt/tpcdi/metrics` | Path to save metrics JSON |
| `--log-detailed-stats` | ❌ No | `false` | Enable per-table timing and record counts |
| `--workspace-path` | ❌ No | - | Workspace path prefix for notebooks (e.g., `/Workspace/Repos/user/repo/databricks`) |
| `--data-gen-notebook` | ❌ No | `generate_tpcdi_data_notebook` | Data generation notebook path (relative to `--workspace-path`) |
| `--benchmark-notebook` | ❌ No | `benchmark_databricks_notebook` | Benchmark notebook path (relative to `--workspace-path`) |
| `--spark-version` | ❌ No | `14.3.x-scala2.12` | Databricks Runtime version (for new jobs) |
| `--cloud` | ❌ No | `AWS` | Cloud provider: `AWS`, `GCP`, or `Azure` (for new jobs) |
| `--node-type-id` | ❌ No | Auto (GCP: `n2d-standard-16`, AWS: `i3.xlarge`) | Worker node type (for new jobs) |
| `--driver-node-type-id` | ❌ No | Same as `--node-type-id` | Driver node type (for new jobs) |
| `--num-workers` | ❌ No | Auto (SF=10→2, SF=100→3, SF=1000→5) | Number of worker nodes (for new jobs) |
| `--existing-cluster-id` | ❌ No | - | Use existing cluster ID instead of creating new (for new jobs) |
| `--cluster-instance-type` | ❌ No | Auto-detected | Worker instance type for metrics logging |
| `--cluster-worker-count` | ❌ No | Auto-detected | Number of worker instances for metrics logging |
| `--cluster-master-type` | ❌ No | Auto-detected | Driver instance type for metrics logging |

### Local Execution Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--raw-data-path` | ❌ No | `.` | Path to raw TPC-DI data (local or `gs://`) |
| `--output-path` | ❌ No | - | Output path (for Databricks platform) |
| `--target-database` | ❌ No | `tpcdi_warehouse` | Target database name |
| `--target-schema` | ❌ No | `dw` | Target schema name |
| `--target-catalog` | ❌ No | - | Unity Catalog name (for Databricks platform) |
| `--batch-id` | ❌ No | - | Batch ID for incremental loads |
| `--metrics-output` | ❌ No | `./metrics` | Path to save metrics JSON |
| `--log-detailed-stats` | ❌ No | `false` | Enable per-table timing and record counts |
| `--gcs-bucket` | ❌ No | - | GCS bucket (required if `--raw-data-path` is `gs://`) |
| `--project-id` | ❌ No | `GOOGLE_CLOUD_PROJECT` env var | GCP project ID (required for Dataproc platform) |
| `--region` | ❌ No | `us-central1` | GCP region |
| `--service-account-email` | ❌ No | - | Service account email for GCS |
| `--service-account-key-file` | ❌ No | - | Path to service account JSON key file |
| `--format` | ❌ No | `parquet` | Table format: `delta` or `parquet` |
| `--cluster-instance-type` | ❌ No | Auto-detected | Worker instance type for metrics logging |
| `--cluster-worker-count` | ❌ No | Auto-detected | Number of worker instances for metrics logging |
| `--cluster-master-type` | ❌ No | Auto-detected | Driver/master instance type for metrics logging |

**Note:** Platform is auto-detected from `--raw-data-path`: `gs://` paths → Dataproc platform, others → Databricks platform.

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
