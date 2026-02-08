#!/bin/bash
# Run TPC-DI benchmark on Dataproc. No spaces after backslashes (line continuation).
# For batch load, omit --batch-id (it's only for incremental).

gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=sumit-dataproc-n2dstand16 \
  --region=us-central1 \
  --project=gcp-sandbox-field-eng \
  --py-files=benchmark.zip \
  --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
  -- \
  --load-type batch \
  --scale-factor 1000 \
  --format delta \
  --gcs-bucket=sumit_prakash_gcs \
  --project-id=gcp-sandbox-field-eng \
  --region=us-central1 \
  --raw-data-path=gs://sumit_prakash_gcs/tpcdi \
  --target-database=tpcdi_warehouse \
  --target-schema=dw \
  --save-metrics \
  --metrics-output=gs://sumit_prakash_gcs/tpcdi/metrics \
  --service-account-email=sumit-wmt-workspace-creator@gcp-sandbox-field-eng.iam.gserviceaccount.com \
  --service-account-key-file=gs://sumit_prakash_gcs/service_account_key_file/service_account.json
