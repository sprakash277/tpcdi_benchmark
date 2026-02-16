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

Arguments can be passed as environment variables or as key=value on the command line
(e.g. cluster=my-cluster region=us-central1 project=my-proj ...). Use hyphens in
keys: load-type, scale-factor, gcs-bucket, raw-data-path, metrics-output,
service-account-email, service-account-key-file, batch-id, metastore-service, deps-bucket, use-serverless.

Required:
  cluster                  Dataproc cluster name (required for cluster mode; omit when use-serverless=1)
  region                   GCP region (e.g. us-central1)
  project                  GCP project ID
  load-type                batch | incremental
  scale-factor             Scale factor (e.g. 10, 100, 1000)
  gcs-bucket               GCS bucket name
  raw-data-path            Base path to raw data (e.g. gs://bucket/tpcdi)
  metrics-output           Path to write metrics (e.g. gs://bucket/tpcdi/metrics)
  service-account-email   Service account email for GCS/job execution
  service-account-key-file Path to SA key JSON (local or gs://)

Conditional:
  batch-id                 Required when load-type=incremental

Optional:
  deps-bucket              For serverless; defaults to gcs-bucket
  metastore-service        Dataproc Metastore, e.g. projects/PROJECT/locations/REGION/services/SERVICE
  batch-wait-log-file      Path to file to save gcloud dataproc batches wait output (serverless only)
  use-serverless           Set to 1 for serverless batch + wait + fetch usage

Example (cluster, key=value):
  ./run_dataproc_job.sh cluster=my-cluster region=us-central1 project=my-proj \
    load-type=batch scale-factor=10 gcs-bucket=my-bucket raw-data-path=gs://my-bucket/tpcdi \
    metrics-output=gs://my-bucket/metrics \
    service-account-email=sa@proj.iam.gserviceaccount.com \
    service-account-key-file=gs://my-bucket/key.json

Example (env vars):
  CLUSTER=my-cluster REGION=us-central1 ... ./run_dataproc_job.sh
EOF
}

# -----------------------------------------------------------------------------
# Helper: validate required arguments; exit 1 with message if invalid
# -----------------------------------------------------------------------------
validate_required_args() {
  local missing=""
  # Cluster required only for cluster mode; not for serverless
  [ "${USE_SERVERLESS}" != "1" ] && [ -z "${CLUSTER}" ] && missing="${missing} cluster"
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
  _batch_opts=(
    --region="${REGION}"
    --project="${PROJECT}"
    --deps-bucket="${DEPS_BUCKET}"
    --py-files=benchmark.zip
    --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar
    --service-account="${SERVICE_ACCOUNT_EMAIL}"
  )
  [ -n "${METASTORE_SERVICE}" ] && _batch_opts+=(--metastore-service="${METASTORE_SERVICE}")
  submit_output=$(cd "${project_root}" && gcloud dataproc batches submit pyspark run_benchmark_dataproc.py \
    "${_batch_opts[@]}" \
    -- \
    "${SCRIPT_ARGS[@]}" \
    2>&1)
  local submit_rc=$?
  echo "${submit_output}"
  [ -n "${BATCH_WAIT_LOG_FILE}" ] && printf '%s\n' "=== gcloud dataproc batches submit output ===" "${submit_output}" >> "${BATCH_WAIT_LOG_FILE}"

  if [ ${submit_rc} -ne 0 ]; then
    echo "gcloud dataproc batches submit failed (exit code ${submit_rc}). Check output above." 1>&2
    return 1
  fi

  local batch_id
  batch_id=$(parse_batch_id_from_output "${submit_output}")
  if [ -z "${batch_id}" ]; then
    echo "Could not parse batch ID from submit output. Cannot run fetch step." 1>&2
    echo "Submit output was printed above; if using batch-wait-log-file, see that file for full log." 1>&2
    return 1
  fi
  echo "Batch ID: ${batch_id}"

  echo "Waiting for batch to complete..."
  if [ -n "${BATCH_WAIT_LOG_FILE}" ]; then
    echo "Sending batches wait output to ${BATCH_WAIT_LOG_FILE}"
    gcloud dataproc batches wait "${batch_id}" --region="${REGION}" --project="${PROJECT}" 2>&1 | tee -a "${BATCH_WAIT_LOG_FILE}"
  else
    gcloud dataproc batches wait "${batch_id}" --region="${REGION}" --project="${PROJECT}"
  fi

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
  _job_opts=(
    --cluster="${CLUSTER}"
    --region="${REGION}"
    --project="${PROJECT}"
    --py-files=benchmark.zip
    --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar
  )
  [ -n "${METASTORE_SERVICE}" ] && _job_opts+=(--metastore-service="${METASTORE_SERVICE}")
  (cd "${project_root}" && gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
    "${_job_opts[@]}" \
    -- \
    "${SCRIPT_ARGS[@]}")
}

# -----------------------------------------------------------------------------
# Helper: parse key=value arguments and export as ENV_VAR (key with - -> _ and upper)
# -----------------------------------------------------------------------------
parse_key_value_args() {
  local arg key key_upper val
  for arg in "$@"; do
    case "${arg}" in
      *"="*)
        key="${arg%%=*}"
        val="${arg#*=}"
        key_upper=$(echo "${key}" | sed 's/-/_/g' | tr 'a-z' 'A-Z')
        [ -n "${key_upper}" ] && [ -n "${val}" ] && export "${key_upper}=${val}"
        ;;
    esac
  done
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
[ "${1:-}" = "--help" ] || [ "${1:-}" = "-h" ] && { print_usage; exit 0; }

# Allow key=value on the command line (e.g. cluster=my-cluster region=us-central1)
parse_key_value_args "$@"

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
METASTORE_SERVICE="${METASTORE_SERVICE:-}"
BATCH_WAIT_LOG_FILE="${BATCH_WAIT_LOG_FILE:-}"

validate_required_args || exit 1

echo "Removing old benchmark.zip and zipping benchmark/..."
ensure_benchmark_zip "${PROJECT_ROOT}" || exit 1

if [ "${USE_SERVERLESS}" = "1" ]; then
  run_serverless "${PROJECT_ROOT}"
else
  run_cluster_job "${PROJECT_ROOT}"
fi
