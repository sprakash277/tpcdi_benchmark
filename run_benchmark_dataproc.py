#!/usr/bin/env python3
"""
Dataproc benchmark runner for TPC-DI.
Run this script on Dataproc to benchmark ETL performance.
"""

import sys
from pathlib import Path

# Add benchmark module to path
sys.path.insert(0, str(Path(__file__).parent))

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
    parser.add_argument("--metrics-output",
                       help="Path to save metrics JSON (default: gs://<bucket>/tpcdi/metrics)")
    
    args = parser.parse_args()
    
    # Construct raw data path
    if args.raw_data_path:
        raw_data_path = args.raw_data_path
    else:
        raw_data_path = f"gs://{args.gcs_bucket}/tpcdi/sf={args.scale_factor}"
    
    # Construct metrics output path
    if args.metrics_output:
        metrics_output = args.metrics_output
    else:
        metrics_output = f"gs://{args.gcs_bucket}/tpcdi/metrics"
    
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
        metrics_output_path=metrics_output,
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
