# How to run the TPC-DI benchmark on Dataproc

This guide lists all parameters for the Dataproc benchmark, explains which are mandatory vs optional, and what each means. See **[BENCHMARK_README.md](BENCHMARK_README.md)** for overview, prerequisites, and Databricks usage.

---

## 1. All parameters

You run the benchmark with `gcloud dataproc jobs submit pyspark`. Options **before** `--` are gcloud/Spark options; arguments **after** `--` are passed to the benchmark script.

### 1.1 gcloud options (before `--`)

| Option | Description |
|--------|-------------|
| `--cluster` | Dataproc cluster name |
| `--region` | GCP region (e.g. `us-central1`) |
| `--project` | GCP project ID |
| `--py-files=benchmark.zip` | Benchmark package; create with `zip -r benchmark.zip benchmark` |
| `--jars=libs/spark-xml_2.12-0.18.0.jar` | Optional; bundled spark-xml JAR (e.g. when no Maven access) |

### 1.2 Script parameters (after `--`)

| Argument | Default | Description |
|----------|---------|-------------|
| `--load-type` | — | `batch` or `incremental` |
| `--scale-factor` | — | TPC-DI scale factor (e.g. 10, 100, 1000) |
| `--gcs-bucket` | — | GCS bucket for raw data and metrics |
| `--project-id` | — | GCP project ID |
| `--region` | `us-central1` | GCP region |
| `--raw-data-path` | `gs://<gcs-bucket>/tpcdi/sf=<scale-factor>` | Path to raw TPC-DI data in GCS |
| `--target-database` | `tpcdi_warehouse` | Target database name |
| `--target-schema` | `dw` | Target schema name |
| `--batch-id` | — | Batch ID for incremental loads |
| `--spark-master` | `yarn` | Spark master URL |
| `--service-account-email` | — | Service account email for GCS access |
| `--service-account-key-file` | — | Path to SA JSON key file (local or `gs://`) |
| `--save-metrics` | `true` | Save benchmark metrics to GCS |
| `--no-save-metrics` | — | Do not save metrics |
| `--metrics-output` | `gs://<gcs-bucket>/tpcdi/metrics` | Directory to save metrics JSON |
| `--log-detailed-stats` | `false` | Log per-table timing and row counts |
| `--format` | `parquet` | Table format: `delta` or `parquet` |

---

## 2. Mandatory vs optional parameters

### 2.1 Mandatory (always)

- **gcloud:** `--cluster`, `--region`, `--project`, `--py-files=benchmark.zip`
- **Script:** `--load-type`, `--scale-factor`, `--gcs-bucket`, `--project-id`

### 2.2 Mandatory only for incremental

- **Script:** `--batch-id` — required when `--load-type incremental`.

### 2.3 Optional (have defaults)

If omitted, these use the defaults in the table above:

- **gcloud:** `--jars` (no default; omit unless using bundled spark-xml)
- **Script:** `--region`, `--raw-data-path`, `--target-database`, `--target-schema`, `--spark-master`, `--save-metrics` / `--no-save-metrics`, `--metrics-output`, `--log-detailed-stats`, `--format`

### 2.4 Optional (no default)

- **Script:** `--service-account-email`, `--service-account-key-file` — use both when running with a service account for GCS access.

---

## 3. What each parameter means

### gcloud options (before `--`)

| Option | Meaning |
|--------|--------|
| `--cluster` | The Dataproc cluster that runs the PySpark job. |
| `--region` | GCP region where the cluster lives (e.g. `us-central1`). |
| `--project` | GCP project that owns the cluster and job. |
| `--py-files=benchmark.zip` | Zipped `benchmark` package. The script imports `benchmark`; without this, the job fails. Create with `zip -r benchmark.zip benchmark` from the project root. |
| `--jars=libs/spark-xml_2.12-0.18.0.jar` | Spark-xml JAR for reading `CustomerMgmt.xml`. Optional if the cluster can resolve Maven packages; use when air-gapped or without Maven. Run from project root so `libs/...` resolves. |

### Script parameters (after `--`)

| Argument | Meaning |
|----------|--------|
| `--load-type` | **batch** = full historical + Batch1 load; **incremental** = only a specific batch (use with `--batch-id`). |
| `--scale-factor` | TPC-DI scale factor (e.g. 10, 100, 1000). Must match the raw data under `--raw-data-path`. |
| `--gcs-bucket` | GCS bucket used for raw data, metrics, and Spark warehouse (e.g. `gs://<bucket>/spark-warehouse`). |
| `--project-id` | GCP project ID; used for Spark/Hadoop GCS config. |
| `--region` | GCP region; default `us-central1`. |
| `--raw-data-path` | Full GCS path to raw TPC-DI data. Default `gs://<gcs-bucket>/tpcdi/sf=<scale-factor>`. Must already exist. |
| `--target-database` | Spark database name. On Dataproc, effectively `tpcdi_warehouse_dw` (database + schema). |
| `--target-schema` | Schema name; combined with `--target-database` for table naming. |
| `--batch-id` | Batch number for **incremental** loads (e.g. 2, 3, …). Required only when `--load-type incremental`. |
| `--spark-master` | Spark master URL; default `yarn` on Dataproc. |
| `--service-account-email` | Service account email for GCS access. Use together with `--service-account-key-file`. |
| `--service-account-key-file` | Path to the SA JSON key file. **Local** path on the driver → Spark uses this SA for GCS. **`gs://`** path → Spark uses default credentials; key not used for Spark GCS auth. |
| `--save-metrics` | Save benchmark metrics JSON to GCS (default). |
| `--no-save-metrics` | Do not save metrics; results still printed to stdout. |
| `--metrics-output` | GCS directory for metrics JSON when saving. Default `gs://<gcs-bucket>/tpcdi/metrics`. |
| `--log-detailed-stats` | Log per-table timing and row counts; otherwise only job-level summary. |
| `--format` | Table format: **parquet** (default) or **delta**. Use **delta** only if the Delta package is available (benchmark adds it via `spark.jars.packages` when `--format delta`). |

---

## 4. Example: Dataproc run with service account

Full `gcloud` command using a **service account** for GCS, with optional parameters set explicitly where useful.

```bash
# From project root:
zip -r benchmark.zip benchmark

gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=sumitbnchmark \
  --region=us-central1 \
  --project=gcp-sandbox-field-eng \
  --py-files=benchmark.zip \
  --jars=libs/spark-xml_2.12-0.18.0.jar \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --format delta \
  --gcs-bucket=sumit_prakash_gcs \
  --project-id=gcp-sandbox-field-eng \
  --region=us-central1 \
  --raw-data-path=gs://sumit_prakash_gcs/benchmark/__unitystorage/catalogs/059afd06-0390-412b-9f4b-af8a5511fd35/volumes/f91c4ca5-899e-4ce2-8989-bf7627b9dda2/sf=10 \
  --target-database=tpcdi_warehouse \
  --target-schema=dw \
  --spark-master=yarn \
  --save-metrics \
  --metrics-output=gs://sumit_prakash_gcs/tpcdi/metrics \
  --service-account-email=tsumit-wmt-workspace-creator@gcp-sandbox-field-eng.iam.gserviceaccount.com \
  --service-account-key-file=gs://sumit_prakash_gcs/service_account_key_file/service_account.json
```

**Notes:**

- `--jars=libs/spark-xml_2.12-0.18.0.jar`: use bundled spark-xml; run from project root.
- `--format delta`: benchmark adds the Delta package via `spark.jars.packages`.
- `--service-account-key-file=gs://...`: Spark uses **default** GCS credentials (cluster identity) because the key is not local. To have Spark use the SA for GCS, use a **local** key path on the driver (e.g. `--service-account-key-file=/path/on/driver/service_account.json`).
- Add `--log-detailed-stats` after `--` for per-table timing.

**Incremental example** (same gcloud options, script args for batch 2):

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=sumitbnchmark \
  --region=us-central1 \
  --project=gcp-sandbox-field-eng \
  --py-files=benchmark.zip \
  --jars=libs/spark-xml_2.12-0.18.0.jar \
  -- \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --format delta \
  --gcs-bucket=sumit_prakash_gcs \
  --project-id=gcp-sandbox-field-eng \
  --region=us-central1 \
  --raw-data-path=gs://sumit_prakash_gcs/benchmark/__unitystorage/catalogs/059afd06-0390-412b-9f4b-af8a5511fd35/volumes/f91c4ca5-899e-4ce2-8989-bf7627b9dda2/sf=10 \
  --target-database=tpcdi_warehouse \
  --target-schema=dw \
  --save-metrics \
  --metrics-output=gs://sumit_prakash_gcs/tpcdi/metrics \
  --service-account-email=tsumit-wmt-workspace-creator@gcp-sandbox-field-eng.iam.gserviceaccount.com \
  --service-account-key-file=gs://sumit_prakash_gcs/service_account_key_file/service_account.json
```
