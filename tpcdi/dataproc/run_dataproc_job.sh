#!/bin/bash
# Run TPC-DI benchmark on Dataproc. No spaces after backslashes (line continuation).
# For batch load, omit --batch-id (it's only for incremental).
#
# Set USE_SERVERLESS=1 to submit a serverless batch, wait for it, then run
# fetch_dataproc_batch_usage.py in sequence to merge usage into metrics.

set -e

REGION="${REGION:-us-central1}"
PROJECT="${PROJECT:-gcp-sandbox-field-eng}"
GCS_BUCKET="${GCS_BUCKET:-sumit_prakash_gcs}"
METRICS_OUTPUT="${METRICS_OUTPUT:-gs://sumit_prakash_gcs/tpcdi/metrics}"
DEPS_BUCKET="${DEPS_BUCKET:-gs://sumit_prakash_gcs}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "${USE_SERVERLESS}" = "1" ]; then
  # Serverless: submit batch, wait, then fetch usage and merge into metrics (in sequence)
  echo "Submitting serverless batch..."
  SUBMIT_OUTPUT=$(gcloud dataproc batches submit pyspark run_benchmark_dataproc.py \
    --region="${REGION}" \
    --project="${PROJECT}" \
    --deps-bucket="${DEPS_BUCKET}" \
    --py-files=benchmark.zip \
    --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
    -- \
    --load-type batch \
    --scale-factor 1000 \
    --format delta \
    --gcs-bucket="${GCS_BUCKET}" \
    --project-id="${PROJECT}" \
    --region="${REGION}" \
    --raw-data-path=gs://${GCS_BUCKET}/tpcdi \
    --target-database=tpcdi_warehouse \
    --target-schema=dw \
    --save-metrics \
    --metrics-output="${METRICS_OUTPUT}" \
    2>&1)
  echo "${SUBMIT_OUTPUT}"

  BATCH_ID=$(echo "${SUBMIT_OUTPUT}" | sed -n 's/.*\/batches\/\([^[:space:]/]*\).*/\1/p' | tail -1)
  if [ -z "${BATCH_ID}" ]; then
    echo "Could not parse batch ID from submit output. Cannot run fetch step." 1>&2
    exit 1
  fi
  echo "Batch ID: ${BATCH_ID}"

  echo "Waiting for batch to complete..."
  gcloud dataproc batches wait "${BATCH_ID}" --region="${REGION}" --project="${PROJECT}"

  echo "Fetching batch usage and merging into metrics..."
  python "${SCRIPT_DIR}/fetch_dataproc_batch_usage.py" \
    --batch-id "${BATCH_ID}" \
    --region "${REGION}" \
    --project "${PROJECT}" \
    --metrics-output "${METRICS_OUTPUT}"
  echo "Done (submit → wait → fetch)."
else
  # Cluster job (no batch ID; fetch step not applicable)
  gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
    --cluster=sumit-dataproc-n2dstand16 \
    --region="${REGION}" \
    --project="${PROJECT}" \
    --py-files=benchmark.zip \
    --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
    -- \
    --load-type batch \
    --scale-factor 1000 \
    --format delta \
    --gcs-bucket="${GCS_BUCKET}" \
    --project-id="${PROJECT}" \
    --region="${REGION}" \
    --raw-data-path=gs://${GCS_BUCKET}/tpcdi \
    --target-database=tpcdi_warehouse \
    --target-schema=dw \
    --save-metrics \
    --metrics-output="${METRICS_OUTPUT}" \
    --service-account-email=sumit-wmt-workspace-creator@gcp-sandbox-field-eng.iam.gserviceaccount.com \
    --service-account-key-file=gs://${GCS_BUCKET}/service_account_key_file/service_account.json
fi
