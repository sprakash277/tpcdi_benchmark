#!/usr/bin/env python3
"""
Dataproc benchmark runner for TPC-DI.
Run this script on Dataproc to benchmark ETL performance.

Note: This script does NOT generate TPC-DI raw data. Raw data must already exist
in GCS at the path given by --raw-data-path (default: gs://<bucket>/tpcdi/sf=<sf>/).
Generate data separately (e.g. TPC-DI DIGen + upload to GCS, or a separate data-gen job).

When submitting via gcloud, you must provide the benchmark package with --py-files:
  zip -r benchmark.zip benchmark
  gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py --py-files=benchmark.zip ...
"""

import sys
from pathlib import Path

def _ensure_benchmark_on_path():
    """Ensure the benchmark package is importable (local run or Dataproc with --py-files)."""
    try:
        import benchmark  # noqa: F401
        return
    except ModuleNotFoundError:
        pass
    script_dir = Path(__file__).resolve().parent
    # Local run: project root as parent of script
    sys.path.insert(0, str(script_dir))
    # Dataproc: script and benchmark.zip often in same staging dir; add zips so "benchmark" is importable
    for z in script_dir.glob("*.zip"):
        sys.path.insert(0, str(z))
    try:
        import benchmark  # noqa: F401
        return
    except ModuleNotFoundError:
        pass
    print(
        "ERROR: Cannot import 'benchmark'. When submitting to Dataproc, pass the package with --py-files:\n"
        "  zip -r benchmark.zip benchmark\n"
        "  gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py --py-files=benchmark.zip \\\n"
        "    --cluster=... --region=... -- \\\n"
        "    --load-type batch --scale-factor 10 ...\n"
        "For local runs, run from the project root or set PYTHONPATH to the project root.",
        file=sys.stderr,
    )
    sys.exit(1)

_ensure_benchmark_on_path()

from benchmark.config import BenchmarkConfig, Platform, LoadType
from benchmark.runner import run_benchmark

# Example configuration for Dataproc
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run TPC-DI benchmark on Dataproc")
    parser.add_argument("--load-type", choices=["batch", "incremental"], required=True,
                       help="Type of load: batch or incremental")
    parser.add_argument("--scale-factor", type=int, required=True,
                       help="TPC-DI scale factor (e.g., 10, 100, 1000)")
    parser.add_argument("--gcs-bucket", required=True,
                       help="GCS bucket name where raw data is stored")
    parser.add_argument("--project-id", required=True,
                       help="GCP project ID")
    parser.add_argument("--region", default="us-central1",
                       help="GCP region (default: us-central1)")
    parser.add_argument("--raw-data-path", 
                       help="Path to raw TPC-DI data in GCS (default: gs://<bucket>/tpcdi/sf=<sf>)")
    parser.add_argument("--target-database", default="tpcdi_warehouse",
                       help="Target database name (default: tpcdi_warehouse)")
    parser.add_argument("--target-schema", default="dw",
                       help="Target schema name (default: dw)")
    parser.add_argument("--batch-id", type=int,
                       help="Batch ID for incremental loads (required for incremental)")
    parser.add_argument("--spark-master", 
                       help="Spark master URL (default: yarn for Dataproc)")
    parser.add_argument("--service-account-email",
                       help="Service account email for GCS access (optional)")
    parser.add_argument("--service-account-key-file",
                       help="Path to service account JSON key file for GCS access (optional)")
    parser.add_argument("--save-metrics", action="store_true", default=True,
                       help="Save benchmark metrics to GCS (default: True)")
    parser.add_argument("--no-save-metrics", dest="save_metrics", action="store_false",
                       help="Do not save benchmark metrics to GCS")
    parser.add_argument("--metrics-output",
                       help="Path to save metrics JSON when --save-metrics (default: gs://<bucket>/tpcdi/metrics)")
    parser.add_argument("--log-detailed-stats", action="store_true",
                       help="Log per-table timing and records; default is only job start/end/total duration")
    parser.add_argument("--format", choices=["delta", "parquet"], default="parquet",
                       help="Table format for warehouse tables (default: parquet). Use delta only if Delta package is on cluster (e.g. --packages io.delta:delta-spark_2.12:3.0.0).")
    
    args = parser.parse_args()
    
    # Construct raw data path
    if args.raw_data_path:
        raw_data_path = args.raw_data_path
    else:
        raw_data_path = f"gs://{args.gcs_bucket}/tpcdi/sf={args.scale_factor}"
    
    # Construct metrics output path (only used when save_metrics is True)
    if args.save_metrics:
        metrics_output = args.metrics_output if args.metrics_output else f"gs://{args.gcs_bucket}/tpcdi/metrics"
    else:
        metrics_output = args.metrics_output  # can be None when not saving
    
    config = BenchmarkConfig(
        platform=Platform.DATAPROC,
        load_type=LoadType(args.load_type),
        scale_factor=args.scale_factor,
        raw_data_path=raw_data_path,
        target_database=args.target_database,
        target_schema=args.target_schema,
        batch_id=args.batch_id,
        gcs_bucket=args.gcs_bucket,
        project_id=args.project_id,
        region=args.region,
        spark_master=args.spark_master or "yarn",
        service_account_email=args.service_account_email,
        service_account_key_file=args.service_account_key_file,
        enable_metrics=args.save_metrics,
        metrics_output_path=metrics_output,
        log_detailed_stats=args.log_detailed_stats,
        table_format=args.format,
    )
    
    result = run_benchmark(config)
    
    print("\n" + "="*80)
    print("BENCHMARK RESULTS")
    print("="*80)
    print(f"Platform: {result['config']['platform']}")
    print(f"Load Type: {result['config']['load_type']}")
    print(f"Scale Factor: {result['config']['scale_factor']}")
    if result['config']['batch_id']:
        print(f"Batch ID: {result['config']['batch_id']}")
    print(f"\nTotal Duration: {result['metrics']['total_duration_seconds']:.2f} seconds")
    if result['metrics']['summary']:
        print(f"Total Rows Processed: {result['metrics']['summary']['total_rows_processed']:,}")
        print(f"Throughput: {result['metrics']['summary']['throughput_rows_per_second']:.2f} rows/sec")
        print(f"Data Size: {result['metrics']['summary']['total_bytes_processed'] / (1024*1024):.2f} MB")
    print("="*80)
