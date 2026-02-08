# Collecting Performance Stats from Databricks and Dataproc in One Place

## 1. Use a single metrics output path (recommended)

Configure **both** Databricks and Dataproc to write metrics to the **same** location so all runs land in one folder.

### Option A: GCS bucket (works for both)

- **Databricks:** Set the **Metrics Output Path** widget (or workflow parameter) to:
  ```text
  gs://YOUR_BUCKET/tpcdi_benchmark/metrics
  ```
  Ensure the cluster can write to this bucket (service account or cluster config).

- **Dataproc:** Set `--metrics-output` to the same path:
  ```text
  --metrics-output gs://YOUR_BUCKET/tpcdi_benchmark/metrics
  ```

Each run writes a JSON file with a unique name, for example:

- `metrics_databricks_batch_sf10_20260208_202731.json`
- `metrics_dataproc_batch_sf10_20260208_203015.json`

All files stay in one folder; you can list or aggregate them (see below).

### Option B: Local or shared filesystem

If you run both from the same machine or a shared mount, set the same local path for both (e.g. `./metrics` or `/shared/tpcdi_benchmark/metrics`). Dataproc: `--metrics-output ./metrics`; Databricks: set the widget to the same path (only works if that path is visible from the Databricks runtime, e.g. a mounted volume).

---

## 2. Aggregating metrics into one report

Use the provided script to merge all `metrics_*.json` files from one directory into a single comparison (CSV or JSON).

### From a local directory

```bash
python scripts/aggregate_metrics.py --input ./metrics --output ./metrics_comparison.csv
```

### From a GCS path

```bash
# Download metrics from GCS into a temp dir, then aggregate
mkdir -p /tmp/tpcdi_metrics
gsutil -m cp "gs://YOUR_BUCKET/tpcdi_benchmark/metrics/metrics_*.json" /tmp/tpcdi_metrics/
python scripts/aggregate_metrics.py --input /tmp/tpcdi_metrics --output ./metrics_comparison.csv
```

### Output

- **CSV** (default): One row per run with columns such as `platform`, `load_type`, `scale_factor`, `total_duration_seconds`, `throughput_rows_per_second`, `throughput_mb_per_second`, etc., so you can compare Databricks vs Dataproc side by side.
- **JSON** (optional): `--format json` writes a single JSON array of all run dicts.

---

## 3. Summary

| Goal | Action |
|------|--------|
| Capture both platforms in one place | Set the same `metrics_output_path` (e.g. `gs://bucket/tpcdi_benchmark/metrics`) for Databricks and Dataproc. |
| Compare runs | Run `scripts/aggregate_metrics.py` on that folder to produce a single CSV or JSON. |
