#!/bin/bash
# Run TPC-DI v2 on Dataproc (Delta tables). Same pattern as v2/databricks/run_tpcdi_batch.
# Requires: Delta Lake (from Dataproc image), spark-xml JAR (for CustomerMgmt.xml). See libs/README.md.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

CLUSTER="${CLUSTER:-your-cluster}"
REGION="${REGION:-us-central1}"
PROJECT="${PROJECT:-your-project}"
RAW_DATA_PATH="${RAW_DATA_PATH:-gs://your-bucket/tpcdi}"
DATABASE="${DATABASE:-tpcdi_dw}"
SF="${SF:-10}"
LOAD_TYPE="${LOAD_TYPE:-batch}"
BATCH_ID="${BATCH_ID:-1}"
# Optional: service account for GCS (same as v1)
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_EMAIL:-}"
SERVICE_ACCOUNT_KEY_FILE="${SERVICE_ACCOUNT_KEY_FILE:-}"
# Metrics JSON output path (GCS or local); default gs://sumit_prakash_gcs/tpcdi/metrics
METRICS_OUTPUT="${METRICS_OUTPUT:-gs://sumit_prakash_gcs/tpcdi/metrics}"
# Cluster config for metrics (optional; if unset, script auto-detects from GCP metadata / Spark)
CLUSTER_INSTANCE_TYPE="${CLUSTER_INSTANCE_TYPE:-}"
CLUSTER_WORKER_COUNT="${CLUSTER_WORKER_COUNT:-}"
CLUSTER_MASTER_TYPE="${CLUSTER_MASTER_TYPE:-}"

# JARs: spark-xml from v2/dataproc/libs or set SPARK_XML_JAR (Delta is provided by Dataproc image)
SPARK_XML_JAR="${SPARK_XML_JAR:-$SCRIPT_DIR/libs/spark-xml_2.12-0.18.0.jar}"
if [ ! -f "$SPARK_XML_JAR" ]; then
  echo "WARN: spark-xml JAR not found at $SPARK_XML_JAR; CustomerMgmt.xml load may fail. See libs/README.md."
fi
JARS="$SPARK_XML_JAR"

# Package metrics, cost, and sql/ so runner can read SQL files on the cluster (only main script + py-files are uploaded by default)
zip -q -r tpcdi_metrics.zip tpcdi_metrics.py cost.py 2>/dev/null || true
if [ ! -d "sql" ]; then
  echo "ERROR: sql/ directory not found in $SCRIPT_DIR. Run this script from v2/dataproc."
  exit 1
fi
zip -q -r sql.zip sql/ 2>/dev/null || true
if [ ! -f "sql.zip" ]; then
  echo "ERROR: failed to create sql.zip in $SCRIPT_DIR"
  exit 1
fi

gcloud dataproc jobs submit pyspark run_tpcdi_batch.py \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --project="$PROJECT" \
  --py-files=tpcdi_metrics.zip \
  --files=sql.zip \
  --jars="$JARS" \
  -- \
  --database "$DATABASE" \
  --raw-data-path "$RAW_DATA_PATH" \
  --sf "$SF" \
  --load-type "$LOAD_TYPE" \
  --batch-id "$BATCH_ID" \
  --metrics-output "$METRICS_OUTPUT" \
  $([ -n "$SERVICE_ACCOUNT_EMAIL" ] && echo "--service-account-email $SERVICE_ACCOUNT_EMAIL") \
  $([ -n "$SERVICE_ACCOUNT_KEY_FILE" ] && echo "--service-account-key-file $SERVICE_ACCOUNT_KEY_FILE") \
  $([ -n "$CLUSTER_INSTANCE_TYPE" ] && echo "--cluster-instance-type $CLUSTER_INSTANCE_TYPE") \
  $([ -n "$CLUSTER_WORKER_COUNT" ] && echo "--cluster-worker-count $CLUSTER_WORKER_COUNT") \
  $([ -n "$CLUSTER_MASTER_TYPE" ] && echo "--cluster-master-type $CLUSTER_MASTER_TYPE")
