#!/bin/bash
set -e # Exit on error

# --- Global Config ---
PROJECT="gcp-sandbox-field-eng"
REGION="us-central1"
ZONE="us-central1-a"
METASTORE="projects/$PROJECT/locations/$REGION/services/sumit-bnchmark-metastore"
SUBNET="sumit-tpcdi-subnet"
SA_EMAIL="sumit-wmt-workspace-creator@gcp-sandbox-field-eng.iam.gserviceaccount.com"
SA_KEY="gs://sumit_prakash_gcs/service_account_key_file/service_account.json"

# --- The Reusable Function ---
run_tpcdi_cycle() {
    local CLUSTER_NAME=$1
    local NUM_WORKERS=$2
    local WORKER_TYPE=$3
    local SSD_COUNT=$4
    local LOAD_TYPE=$5
    local SF=$6
    local BATCH_ID=$7
    local DB_NAME=$8

    echo "------------------------------------------------------------"
    echo "STARTING: $CLUSTER_NAME | Load: $LOAD_TYPE | SF: $SF"
    echo "------------------------------------------------------------"

    # 1. Cluster Creation
    gcloud dataproc clusters create "$CLUSTER_NAME" \
      --project="$PROJECT" --region="$REGION" --zone="$ZONE" \
      --dataproc-metastore="$METASTORE" --subnet="$SUBNET" \
      --image-version="2.3-debian12" --no-address --enable-component-gateway \
      --master-machine-type="n2d-standard-8" --master-boot-disk-size=100 --num-master-local-ssds=4 \
      --num-workers="$NUM_WORKERS" --worker-machine-type="$WORKER_TYPE" \
      --worker-boot-disk-size=200 --num-worker-local-ssds="$SSD_COUNT" \
      --optional-components=DELTA --scopes=cloud-platform

    # 2. Job Submission (Handles both Batch and Incremental)
    # If BATCH_ID is empty (for batch loads), the script handles it
    sh run_dataproc_job.sh \
        cluster="$CLUSTER_NAME" \
        region="$REGION" \
        project="$PROJECT" \
        load-type="$LOAD_TYPE" \
        batch-id="$BATCH_ID" \
        scale-factor="$SF" \
        target-database="$DB_NAME" \
        gcs-bucket="sumit_prakash_gcs" \
        raw-data-path="gs://sumit_prakash_gcs/tpcdi" \
        metrics-output="gs://sumit_prakash_gcs/tpcdi/metrics_new" \
        service-account-email="$SA_EMAIL" \
        service-account-key-file="$SA_KEY"

    # 3. Cluster Deletion
    echo "Cleaning up: Deleting $CLUSTER_NAME..."
    gcloud dataproc clusters delete "$CLUSTER_NAME" --region="$REGION" --project="$PROJECT" --quiet
}

# ------------------------------------------------------------------------------
# EXECUTION SEQUENCE
# ------------------------------------------------------------------------------

# --- SF-10,000 WORKFLOW ---
# 1. Batch Load (SF 10k)
run_tpcdi_cycle "sumit-dataproc-10000" 6 "n2d-standard-16" 4 "batch" 10000 "" "tpcdi_warehouse_dataproc_bnchmark_final"

# 2. Incremental Loads (SF 10k) - Running 2 and 3 on same cluster for efficiency
# Note: Since the function deletes the cluster, we call a slightly modified version or just run them back-to-back.
# For simplicity in this script, we'll follow your request and create separate cycles.
run_tpcdi_cycle "sumit-dataproc-inc-10000" 5 "n2d-standard-8" 2 "incremental" 10000 2 "tpcdi_warehouse_dataproc_bnchmark_final"
run_tpcdi_cycle "sumit-dataproc-inc-10000-b3" 5 "n2d-standard-8" 2 "incremental" 10000 3 "tpcdi_warehouse_dataproc_bnchmark_final"

# --- SF-1,000 WORKFLOW ---
# 3. Batch Load (SF 1k)
run_tpcdi_cycle "sumit-dataproc-1000" 5 "n2d-standard-8" 2 "batch" 1000 "" "tpcdi_warehouse_dataproc_bnchmark_final"

# 4. Incremental Loads (SF 1k)
run_tpcdi_cycle "sumit-dataproc-inc-1000" 3 "n2d-standard-8" 2 "incremental" 1000 2 "tpcdi_warehouse_dataproc_bnchmark_final"
run_tpcdi_cycle "sumit-dataproc-inc-1000-b3" 3 "n2d-standard-8" 2 "incremental" 1000 3 "tpcdi_warehouse_dataproc_bnchmark_final"

echo "ALL BENCHMARKS COMPLETED SUCCESSFULLY."
