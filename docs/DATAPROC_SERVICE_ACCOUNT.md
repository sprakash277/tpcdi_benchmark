# Running the TPC-DI Benchmark on Dataproc with a Service Account (SA) and Key File

This guide describes how to run the benchmark on **Dataproc** using a **GCP service account (SA)** and its **JSON key file** for GCS access.

## When to use SA + key file

- The cluster’s default identity (cluster SA or ADC) should **not** be used for GCS (e.g. different project, stricter IAM, or audit requirements).
- You want the **job** to use a specific SA for reading raw data and writing tables/metrics, independent of the cluster’s identity.

If the cluster is already created with the desired SA (`gcloud dataproc clusters create ... --service-account=...`), you do **not** need to pass SA or key file to the benchmark; the job will use the cluster’s identity for GCS.

---

## 1. Create a service account and key

### Create the service account

```bash
export PROJECT_ID=your-project-id
export SA_NAME=tpcdi-dataproc
export SA_EMAIL=${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com

gcloud iam service-accounts create ${SA_NAME} \
  --display-name="TPC-DI Dataproc benchmark" \
  --project=${PROJECT_ID}
```

### Grant GCS permissions

Grant the SA access to your GCS bucket (read raw data, write tables/metrics). Example:

```bash
export BUCKET=your-tpcdi-bucket

# Read + list on the bucket (raw data path)
gcloud storage buckets add-iam-policy-binding gs://${BUCKET} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectViewer" \
  --project=${PROJECT_ID}

# Write (tables, metrics) — use objectAdmin or objectCreator as needed
gcloud storage buckets add-iam-policy-binding gs://${BUCKET} \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/storage.objectAdmin" \
  --project=${PROJECT_ID}
```

Adjust roles (e.g. `objectViewer` vs `objectAdmin`) to match your security policy.

### Create a JSON key for the SA

```bash
gcloud iam service-accounts keys create sa-key.json \
  --iam-account=${SA_EMAIL} \
  --project=${PROJECT_ID}
```

**Security:** Do not commit `sa-key.json` to source control. Add it to `.gitignore`.

---

## 2. Pass SA and key to the benchmark

The benchmark script accepts:

| Argument | Description |
|----------|-------------|
| `--service-account-email` | Full SA email (e.g. `tpcdi-dataproc@project.iam.gserviceaccount.com`) |
| `--service-account-key-file` | Path to the SA’s JSON key file |

**Both** must be set for key-file authentication. The runner configures Spark/Hadoop with:

- `fs.gs.auth.type` = `SERVICE_ACCOUNT_JSON_KEYFILE`
- `fs.gs.auth.service.account.email` = your SA email
- `fs.gs.auth.service.account.keyfile` = path to the key file

If only `--service-account-email` is set (no key file), the code uses SA email only (e.g. for impersonation); typically you want both for explicit key-file auth.

---

## 3. Running the job

### Option A: Run the script locally (driver on your machine)

Use a **local path** to the key file:

```bash
python run_benchmark_dataproc.py \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=your-bucket \
  --project-id=your-project-id \
  --region=us-central1 \
  --service-account-email=tpcdi-dataproc@your-project-id.iam.gserviceaccount.com \
  --service-account-key-file=./sa-key.json
```

Incremental example:

```bash
python run_benchmark_dataproc.py \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --gcs-bucket=your-bucket \
  --project-id=your-project-id \
  --region=us-central1 \
  --service-account-email=tpcdi-dataproc@your-project-id.iam.gserviceaccount.com \
  --service-account-key-file=./sa-key.json
```

### Option B: Submit as a Dataproc PySpark job (key file in GCS)

1. Upload the key file to a **restricted** GCS path (e.g. `gs://your-bucket/secrets/tpcdi-sa-key.json`).
2. Ensure the **cluster’s default SA** (or the identity that runs the job) has **read** access to that object (e.g. bucket IAM or object ACL).
3. From the project root, package the benchmark module: `zip -r benchmark.zip benchmark`.
4. Pass the **GCS path** as `--service-account-key-file` and **`--py-files=benchmark.zip`**:

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=your-cluster-name \
  --region=us-central1 \
  --py-files=benchmark.zip \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=your-bucket \
  --project-id=your-project-id \
  --region=us-central1 \
  --service-account-email=tpcdi-dataproc@your-project-id.iam.gserviceaccount.com \
  --service-account-key-file=gs://your-bucket/secrets/tpcdi-sa-key.json
```

The GCS connector can resolve `gs://` paths for the key file when the cluster identity can read that object.

### Option C: Submit as a Dataproc job (key file on cluster local disk)

1. Copy the key file to a path on the **driver node** (e.g. via an init action, startup script, or one-time copy to `/var/lib/tpcdi/sa-key.json`).
2. Restrict file permissions so only the job user can read it (e.g. `chmod 600`).
3. From the project root, package the benchmark module: `zip -r benchmark.zip benchmark`.
4. Pass that **local path** after the `--` and **`--py-files=benchmark.zip`**:

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=your-cluster-name \
  --region=us-central1 \
  --py-files=benchmark.zip \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=your-bucket \
  --project-id=your-project-id \
  --region=us-central1 \
  --service-account-email=tpcdi-dataproc@your-project-id.iam.gserviceaccount.com \
  --service-account-key-file=/var/lib/tpcdi/sa-key.json
```

---

## 4. Optional: raw data path and metrics path

- **Raw data path:** Default is `gs://<gcs-bucket>/tpcdi/sf=<scale_factor>`. Override with `--raw-data-path=gs://bucket/path/to/sf=10`.
- **Metrics output:** Default is `gs://<gcs-bucket>/tpcdi/metrics`. Override with `--metrics-output=gs://bucket/path/metrics`.

The SA must have read access to the raw data path and write access to the metrics (and table output) path.

---

## 5. Security best practices

- **Do not commit** the JSON key to the repo. Add `sa-key.json` (or `*.json`) to `.gitignore`.
- Prefer **Secret Manager**: store the key in a secret and have an init script or job wrapper write it to a temporary path only the job user can read; pass that path as `--service-account-key-file`.
- Use a **dedicated SA** for this benchmark with **minimal roles** (e.g. only the GCS roles above).
- **Rotate keys** periodically; avoid long-lived keys in shared or world-readable locations.
- If the key is in GCS, restrict the bucket (or object) so only the cluster SA or a small set of identities can read it.

---

## 6. Troubleshooting

| Issue | What to check |
|-------|-------------------------------|
| Access denied reading GCS | SA has `roles/storage.objectViewer` (or equivalent) on the raw data path. |
| Access denied writing GCS | SA has write (e.g. `objectAdmin` or `objectCreator`) on the bucket/path used for tables and metrics. |
| Key file not found | Path is correct and the **driver** process can read it (local path or `gs://` readable by cluster identity). |
| Invalid key / auth error | Key file is valid JSON and not corrupted; SA email matches the key’s `client_email`. |

For GCS connector and Hadoop config details, see `benchmark/platforms/dataproc.py` (key-file auth) and `benchmark/runner.py` (Spark session config).
