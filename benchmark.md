# Dataproc benchmark — parameters and sample run

Parameters for the Dataproc benchmark: **gcloud** options (before `--`) and **script** arguments (after `--`).

---

## gcloud options (before `--`)

| Option | Required | Description |
|--------|----------|-------------|
| `--cluster` | Yes | Dataproc cluster name |
| `--region` | Yes | GCP region (e.g. `us-central1`) |
| `--project` | Yes | GCP project ID |
| `--py-files=benchmark.zip` | Yes | Benchmark package; create with `zip -r benchmark.zip benchmark` |
| `--jars=libs/spark-xml_2.12-0.18.0.jar` | No | Optional; use bundled spark-xml JAR (e.g. when no Maven access) |

---

## Script parameters (after `--`)

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--load-type` | Yes | — | `batch` or `incremental` |
| `--scale-factor` | Yes | — | TPC-DI scale factor (e.g. 10, 100, 1000) |
| `--gcs-bucket` | Yes | — | GCS bucket for raw data and metrics |
| `--project-id` | Yes | — | GCP project ID |
| `--region` | No | `us-central1` | GCP region |
| `--raw-data-path` | No | `gs://<gcs-bucket>/tpcdi/sf=<scale-factor>` | Path to raw TPC-DI data in GCS |
| `--target-database` | No | `tpcdi_warehouse` | Target database name (used with `--target-schema` as `tpcdi_warehouse_dw` on Dataproc) |
| `--target-schema` | No | `dw` | Target schema name |
| `--batch-id` | Yes if `--load-type incremental` | — | Batch ID for incremental loads |
| `--spark-master` | No | `yarn` | Spark master URL |
| `--service-account-email` | No | — | Service account email for GCS access (use with `--service-account-key-file`) |
| `--service-account-key-file` | No | — | Path to SA JSON key file (local or `gs://`); for Spark GCS auth use a **local** path on the driver |
| `--save-metrics` | No | `true` | Save benchmark metrics to GCS |
| `--no-save-metrics` | No | — | Do not save metrics |
| `--metrics-output` | No | `gs://<gcs-bucket>/tpcdi/metrics` | Directory to save metrics JSON (when saving) |
| `--log-detailed-stats` | No | `false` | Log per-table timing and row counts |
| `--format` | No | `parquet` | Table format: `delta` or `parquet` |

---

## Example: Dataproc run with service account

Full `gcloud` example using a **service account** (SA) for GCS access, with all optional parameters set explicitly to their defaults where useful.

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

- `--jars=libs/spark-xml_2.12-0.18.0.jar`: uses the bundled spark-xml JAR; run from project root.
- `--format delta`: Delta package is added via `spark.jars.packages` when the benchmark creates the Spark session.
- `--service-account-key-file=gs://...`: Spark uses **default** GCS credentials (cluster identity) because the key path is not local. For Spark to use the SA for GCS, use a **local** key path on the driver (e.g. copy key to node and pass `--service-account-key-file=/path/on/driver/service_account.json`).
- Omitted `--log-detailed-stats`: only job-level timing is logged; add it for per-table stats.

**Incremental example** (same gcloud options, different script args):

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
