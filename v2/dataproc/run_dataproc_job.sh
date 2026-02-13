#!/bin/bash
# Run TPC-DI v2 on Dataproc (Delta tables). Same pattern as v2/databricks/run_tpcdi_batch.
# Requires: Delta Lake JAR, spark-xml JAR (for CustomerMgmt.xml). JARs: use libs/ or GCS (see libs/README.md).

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

# JARs: Delta Lake (GCS or set DELTA_JAR); spark-xml from v2/dataproc/libs or set SPARK_XML_JAR
DELTA_JAR="${DELTA_JAR:-gs://spark-lib/delta/delta-core_2.12-2.4.0.jar}"
SPARK_XML_JAR="${SPARK_XML_JAR:-$SCRIPT_DIR/libs/spark-xml_2.12-0.18.0.jar}"
if [ ! -f "$SPARK_XML_JAR" ]; then
  echo "WARN: spark-xml JAR not found at $SPARK_XML_JAR; CustomerMgmt.xml load may fail. See libs/README.md."
fi
JARS="$DELTA_JAR,$SPARK_XML_JAR"

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
  --properties=spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog,spark.delta.logStore.gs.impl=io.delta.storage.GCSLogStore,spark.sql.defaultSerializer=org.apache.spark.serializer.KryoSerializer \
  -- \
  --database "$DATABASE" \
  --raw-data-path "$RAW_DATA_PATH" \
  --sf "$SF" \
  --load-type "$LOAD_TYPE" \
  --batch-id "$BATCH_ID" \
  $([ -n "$SERVICE_ACCOUNT_EMAIL" ] && echo "--service-account-email $SERVICE_ACCOUNT_EMAIL") \
  $([ -n "$SERVICE_ACCOUNT_KEY_FILE" ] && echo "--service-account-key-file $SERVICE_ACCOUNT_KEY_FILE")
