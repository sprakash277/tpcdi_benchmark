#!/bin/bash
# Run TPC-DI benchmark on Dataproc. No spaces after backslashes (line continuation).
# For batch load, omit --batch-id (it's only for incremental).
#
# Required env (or pass as VAR=value before the script): cluster, region, project,
# load-type, scale-factor, gcs-bucket, raw-data-path, metrics-output,
# service-account-email, service-account-key-file. If load-type is incremental,
# batch-id is also required.
#
# Set USE_SERVERLESS=1 to submit a serverless batch, wait for it, then run
# fetch_dataproc_batch_usage.py in sequence to merge usage into metrics.
#
# Usage: run with env vars set, or: ./run_dataproc_job.sh --help

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# -----------------------------------------------------------------------------
# Helper: print usage and exit 0
# -----------------------------------------------------------------------------
print_usage() {
  cat <<'EOF'
Run TPC-DI benchmark on Dataproc (cluster or serverless).

Required environment variables:
  CLUSTER                  Dataproc cluster name (for cluster mode)
  REGION                   GCP region (e.g. us-central1)
  PROJECT                  GCP project ID
  LOAD_TYPE                batch | incremental
  SCALE_FACTOR             Scale factor (e.g. 10, 100, 1000)
  GCS_BUCKET               GCS bucket name
  RAW_DATA_PATH            Base path to raw data (e.g. gs://bucket/tpcdi)
  METRICS_OUTPUT           Path to write metrics (e.g. gs://bucket/tpcdi/metrics)
  SERVICE_ACCOUNT_EMAIL    Service account email for GCS/job execution
  SERVICE_ACCOUNT_KEY_FILE Path to SA key JSON (local or gs://)

Conditional:
  BATCH_ID                 Required when LOAD_TYPE=incremental

Optional:
  DEPS_BUCKET              For serverless; defaults to GCS_BUCKET
  USE_SERVERLESS           Set to 1 for serverless batch + wait + fetch usage

Example (cluster):
  CLUSTER=my-cluster REGION=us-central1 PROJECT=my-proj LOAD_TYPE=batch \
  SCALE_FACTOR=10 GCS_BUCKET=my-bucket RAW_DATA_PATH=gs://my-bucket/tpcdi \
  METRICS_OUTPUT=gs://my-bucket/metrics \
  SERVICE_ACCOUNT_EMAIL=sa@proj.iam.gserviceaccount.com \
  SERVICE_ACCOUNT_KEY_FILE=gs://my-bucket/key.json \
  ./run_dataproc_job.sh

Example (serverless, then fetch usage):
  USE_SERVERLESS=1 CLUSTER=dummy REGION=us-central1 ... ./run_dataproc_job.sh
EOF
}

# -----------------------------------------------------------------------------
# Helper: validate required arguments; exit 1 with message if invalid
# -----------------------------------------------------------------------------
validate_required_args() {
  local missing=""
  [ -z "${CLUSTER}" ] && missing="${missing} cluster"
  [ -z "${REGION}" ] && missing="${missing} region"
  [ -z "${PROJECT}" ] && missing="${missing} project"
  [ -z "${LOAD_TYPE}" ] && missing="${missing} load-type"
  [ -z "${SCALE_FACTOR}" ] && missing="${missing} scale-factor"
  [ -z "${GCS_BUCKET}" ] && missing="${missing} gcs-bucket"
  [ -z "${RAW_DATA_PATH}" ] && missing="${missing} raw-data-path"
  [ -z "${METRICS_OUTPUT}" ] && missing="${missing} metrics-output"
  [ -z "${SERVICE_ACCOUNT_EMAIL}" ] && missing="${missing} service-account-email"
  [ -z "${SERVICE_ACCOUNT_KEY_FILE}" ] && missing="${missing} service-account-key-file"
  if [ "${LOAD_TYPE}" = "incremental" ] && [ -z "${BATCH_ID_ARG}" ]; then
    missing="${missing} batch-id (required when load-type is incremental)"
  fi
  if [ -n "${missing}" ]; then
    echo "Missing required arguments:${missing}." 1>&2
    echo "Run with --help for usage." 1>&2
    return 1
  fi
  return 0
}

# -----------------------------------------------------------------------------
# Helper: build benchmark script args (after --). Pass include_sa=1 for cluster.
# Output: array SCRIPT_ARGS (caller uses "${SCRIPT_ARGS[@]}")
# -----------------------------------------------------------------------------
build_benchmark_script_args() {
  local include_sa="${1:-0}"
  SCRIPT_ARGS=(
    --load-type "${LOAD_TYPE}"
    --scale-factor "${SCALE_FACTOR}"
    --format delta
    --gcs-bucket="${GCS_BUCKET}"
    --project-id="${PROJECT}"
    --region="${REGION}"
    --raw-data-path="${RAW_DATA_PATH}"
    --target-database=tpcdi_warehouse
    --target-schema=dw
    --save-metrics
    --metrics-output="${METRICS_OUTPUT}"
  )
  [ -n "${BATCH_ID_ARG}" ] && SCRIPT_ARGS+=(--batch-id "${BATCH_ID_ARG}")
  if [ "${include_sa}" = "1" ]; then
    SCRIPT_ARGS+=(--service-account-email="${SERVICE_ACCOUNT_EMAIL}")
    SCRIPT_ARGS+=(--service-account-key-file="${SERVICE_ACCOUNT_KEY_FILE}")
  fi
}

# -----------------------------------------------------------------------------
# Helper: parse batch ID from gcloud batches submit output
# -----------------------------------------------------------------------------
parse_batch_id_from_output() {
  local output="$1"
  echo "${output}" | sed -n 's/.*\/batches\/\([^[:space:]/]*\).*/\1/p' | tail -1
}

# -----------------------------------------------------------------------------
# Helper: remove benchmark.zip and create fresh zip of benchmark/ (in PROJECT_ROOT).
# Call once per run so each job uses current benchmark code.
# -----------------------------------------------------------------------------
ensure_benchmark_zip() {
  local root="${1:?PROJECT_ROOT required}"
  if [ ! -d "${root}/benchmark" ]; then
    echo "Directory ${root}/benchmark not found. Cannot create benchmark.zip." 1>&2
    return 1
  fi
  (cd "${root}" && rm -f benchmark.zip && zip -r benchmark.zip benchmark -x '*.pyc' -x '*/__pycache__/*')
}

# -----------------------------------------------------------------------------
# Helper: run serverless (submit → wait → fetch usage)
# -----------------------------------------------------------------------------
run_serverless() {
  local project_root="${1:?PROJECT_ROOT required}"
  echo "Submitting serverless batch..."
  build_benchmark_script_args 0
  local submit_output
  submit_output=$(cd "${project_root}" && gcloud dataproc batches submit pyspark run_benchmark_dataproc.py \
    --region="${REGION}" \
    --project="${PROJECT}" \
    --deps-bucket="${DEPS_BUCKET}" \
    --py-files=benchmark.zip \
    --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
    --service-account="${SERVICE_ACCOUNT_EMAIL}" \
    -- \
    "${SCRIPT_ARGS[@]}" \
    2>&1)
  echo "${submit_output}"

  local batch_id
  batch_id=$(parse_batch_id_from_output "${submit_output}")
  if [ -z "${batch_id}" ]; then
    echo "Could not parse batch ID from submit output. Cannot run fetch step." 1>&2
    return 1
  fi
  echo "Batch ID: ${batch_id}"

  echo "Waiting for batch to complete..."
  gcloud dataproc batches wait "${batch_id}" --region="${REGION}" --project="${PROJECT}"

  echo "Fetching batch usage and merging into metrics..."
  python "${SCRIPT_DIR}/fetch_dataproc_batch_usage.py" \
    --batch-id "${batch_id}" \
    --region "${REGION}" \
    --project "${PROJECT}" \
    --metrics-output "${METRICS_OUTPUT}"
  echo "Done (submit → wait → fetch)."
}

# -----------------------------------------------------------------------------
# Helper: run cluster job (submit pyspark to cluster)
# -----------------------------------------------------------------------------
run_cluster_job() {
  local project_root="${1:?PROJECT_ROOT required}"
  build_benchmark_script_args 1
  (cd "${project_root}" && gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
    --cluster="${CLUSTER}" \
    --region="${REGION}" \
    --project="${PROJECT}" \
    --py-files=benchmark.zip \
    --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
    -- \
    "${SCRIPT_ARGS[@]}")
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
[ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ] && { print_usage; exit 0; }

PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CLUSTER="${CLUSTER:-}"
REGION="${REGION:-}"
PROJECT="${PROJECT:-}"
LOAD_TYPE="${LOAD_TYPE:-}"
BATCH_ID_ARG="${BATCH_ID:-}"
SCALE_FACTOR="${SCALE_FACTOR:-}"
GCS_BUCKET="${GCS_BUCKET:-}"
RAW_DATA_PATH="${RAW_DATA_PATH:-}"
METRICS_OUTPUT="${METRICS_OUTPUT:-}"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_EMAIL:-}"
SERVICE_ACCOUNT_KEY_FILE="${SERVICE_ACCOUNT_KEY_FILE:-}"
DEPS_BUCKET="${DEPS_BUCKET:-$GCS_BUCKET}"

validate_required_args || exit 1

echo "Removing old benchmark.zip and zipping benchmark/..."
ensure_benchmark_zip "${PROJECT_ROOT}" || exit 1

if [ "${USE_SERVERLESS}" = "1" ]; then
  run_serverless "${PROJECT_ROOT}"
else
  run_cluster_job "${PROJECT_ROOT}"
fi
