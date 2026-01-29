# How to run the TPC-DI benchmark on Dataproc

This guide describes how to run the benchmark on **Dataproc**: prerequisites, all parameters (mandatory vs optional, what each means), setup, run commands, and running with a service account. See **[BENCHMARK_README.md](BENCHMARK_README.md)** for overview, architecture, and Databricks usage.

---

## Prerequisites

1. **Dataproc cluster** with GCS connector installed.
2. **TPC-DI raw data** must already exist in GCS. `run_benchmark_dataproc.py` does **not** generate data. Generate separately (e.g. TPC-DI DIGen, then upload to GCS).
3. Data path: `gs://<bucket>/tpcdi/sf=<scale_factor>/` (or your `--raw-data-path`).
4. GCP **project ID** and **region**.
5. **Metastore (optional):** Without a [Dataproc Metastore](https://cloud.google.com/dataproc-metastore/docs), Spark uses the default metastore. The benchmark sets the warehouse to GCS (`gs://<bucket>/spark-warehouse`) and uses two-part table names (`database.table`) and Parquet by default. See **docs/DATAPROC_METASTORE.md**.

**How this guide is structured:** §1 lists all parameters (gcloud + script). §2 explains which are mandatory vs optional. §3 describes what each parameter means. §4–§5 cover setup and run commands (batch, incremental, `--log-detailed-stats`). §6 explains running with a service account. §7 gives a full example with SA. The end section covers Dataproc-specific troubleshooting.

---

## 1. All parameters

You run the benchmark with `gcloud dataproc jobs submit pyspark`. Options **before** `--` are gcloud/Spark; arguments **after** `--` go to the benchmark script.

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

If omitted, these use the defaults in §1:

- **gcloud:** `--jars` (omit unless using bundled spark-xml)
- **Script:** `--region`, `--raw-data-path`, `--target-database`, `--target-schema`, `--spark-master`, `--save-metrics` / `--no-save-metrics`, `--metrics-output`, `--log-detailed-stats`, `--format`

### 2.4 Optional (no default)

- **Script:** `--service-account-email`, `--service-account-key-file` — use both when running with a service account for GCS access.

---

## 3. What each parameter means

### gcloud options (before `--`)

| Option | Meaning |
|--------|--------|
| `--cluster` | Dataproc cluster that runs the PySpark job. |
| `--region` | GCP region where the cluster lives (e.g. `us-central1`). |
| `--project` | GCP project that owns the cluster and job. |
| `--py-files=benchmark.zip` | Zipped `benchmark` package. The script imports `benchmark`; without this, the job fails. Create with `zip -r benchmark.zip benchmark` from the project root. |
| `--jars=libs/spark-xml_2.12-0.18.0.jar` | Spark-xml JAR for reading `CustomerMgmt.xml`. Optional if the cluster can resolve Maven packages; use when air-gapped or without Maven. Run from project root. |

### Script parameters (after `--`)

| Argument | Meaning |
|----------|--------|
| `--load-type` | **batch** = full historical + Batch1; **incremental** = specific batch (use with `--batch-id`). |
| `--scale-factor` | TPC-DI scale factor. Must match raw data under `--raw-data-path`. |
| `--gcs-bucket` | GCS bucket for raw data, metrics, and Spark warehouse. |
| `--project-id` | GCP project ID for Spark/Hadoop GCS config. |
| `--region` | GCP region; default `us-central1`. |
| `--raw-data-path` | Full GCS path to raw TPC-DI data. Default `gs://<gcs-bucket>/tpcdi/sf=<scale-factor>`. Must exist. |
| `--target-database` | Spark database name. On Dataproc, used as `tpcdi_warehouse_dw` with schema. |
| `--target-schema` | Schema name; combined with `--target-database` for table naming. |
| `--batch-id` | Batch number for **incremental** loads. Required only when `--load-type incremental`. |
| `--spark-master` | Spark master URL; default `yarn`. |
| `--service-account-email` | Service account email for GCS. Use with `--service-account-key-file`. |
| `--service-account-key-file` | Path to SA JSON key. **Local** path → Spark uses SA for GCS. **`gs://`** path → Spark uses default credentials. |
| `--save-metrics` | Save metrics JSON to GCS (default). |
| `--no-save-metrics` | Do not save metrics; results still printed to stdout. |
| `--metrics-output` | GCS directory for metrics JSON. Default `gs://<gcs-bucket>/tpcdi/metrics`. |
| `--log-detailed-stats` | Log per-table timing and row counts; otherwise job-level summary only. |
| `--format` | **parquet** (default) or **delta**. With `--format delta`, the benchmark adds the Delta package. |

---

## 4. Setup before running

**Package the benchmark module:** Only the main script is uploaded by default. Provide the `benchmark` package via `--py-files`. From the project root:

```bash
zip -r benchmark.zip benchmark
```

Pass `--py-files=benchmark.zip` (or `--py-files=gs://<bucket>/benchmark.zip` if uploaded to GCS) to every `gcloud dataproc jobs submit pyspark` command.

**Metrics:** By default, metrics are saved to GCS. Use `--no-save-metrics` to skip; use `--metrics-output=gs://bucket/path/metrics` to override the default path.

**Table format:** Use `--format delta` or `--format parquet` (default). With `--format delta`, the benchmark adds the Delta package automatically.

**Spark packages:** The benchmark adds `spark-xml` (for CustomerMgmt.xml) and, when `--format delta`, Delta automatically. The driver needs Maven access. For air-gapped setups, use `--jars=libs/spark-xml_2.12-0.18.0.jar` (see **libs/README.md**).

---

## 5. Run commands

### Batch load

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --project=<your-project> \
  --py-files=benchmark.zip \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1
```

### With per-table timing (`--log-detailed-stats`)

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --project=<your-project> \
  --py-files=benchmark.zip \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1 \
  --log-detailed-stats
```

### Incremental load

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --project=<your-project> \
  --py-files=benchmark.zip \
  -- \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1
```

---

## 6. Running with a service account (SA)

When the cluster’s default identity should not be used for GCS (e.g. different project, stricter IAM), run with a **service account** and its **JSON key file**.

**1. Create a service account and key**

- Create a SA (e.g. `tpcdi-dataproc@<project>.iam.gserviceaccount.com`).
- Grant the SA roles to read/write your GCS bucket (e.g. **Storage Object Viewer** on raw data, **Storage Object Admin** or **Creator** where you write).
- Create a JSON key:
  ```bash
  gcloud iam service-accounts keys create sa-key.json \
    --iam-account=tpcdi-dataproc@<project>.iam.gserviceaccount.com
  ```

**2. Pass SA and key to the benchmark**

- `--service-account-email <sa-email>`
- `--service-account-key-file <path-to-json>`

Use **both** for key-file auth.

**3. Run locally (driver on your machine)**

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

**4. Submit as Dataproc job**

**A. Key file in GCS**  
Upload the key to a restricted GCS path (e.g. `gs://<bucket>/secrets/tpcdi-sa-key.json`). Ensure the cluster’s default SA can read it. Then:

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  --project=<your-project> \
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

**B. Key file on cluster disk**  
Copy the key to the driver (e.g. via init action to `/var/lib/tpcdi/sa-key.json`). Use the same `gcloud` command as above with `--service-account-key-file=/var/lib/tpcdi/sa-key.json` after `--`.

**5. Using only the cluster’s service account**

If the cluster is created with the desired SA (`gcloud dataproc clusters create ... --service-account=...`), you do **not** need `--service-account-email` or `--service-account-key-file`. The job uses the cluster’s default identity for GCS.

**6. Security**

- Do **not** commit the JSON key. Add it to `.gitignore`.
- Prefer **Secret Manager** or a restricted GCS path; restrict access to the key.
- Use a dedicated SA with minimal roles; rotate keys regularly.

For more detail, see **docs/DATAPROC_SERVICE_ACCOUNT.md**.

---

## 7. Full example: Dataproc run with service account

Complete `gcloud` command with SA, optional parameters set explicitly:

```bash
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

**Notes:** `--jars=libs/...` uses the bundled spark-xml JAR; run from project root. `--format delta` adds the Delta package. With `--service-account-key-file=gs://...`, Spark uses **default** GCS credentials (key not local). For Spark to use the SA, use a **local** key path on the driver. Add `--log-detailed-stats` for per-table timing.

**Incremental (batch 2):**

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

---

## Troubleshooting (Dataproc)

- **GCS connector:** Ensure it is installed on the cluster.
- **GCS permissions:** Verify bucket (and paths) access for the identity used (cluster SA or SA key).
- **Raw data:** Confirm `--raw-data-path` exists and is readable from the cluster.
- **Project / region:** Check `--project-id` and `--region` match the cluster.
- **Tables/database gone after job or cluster ends:** Without a Dataproc Metastore, metadata and often data are ephemeral. See **docs/DATAPROC_METASTORE.md**.
