#!/usr/bin/env python3
"""
Databricks benchmark runner for TPC-DI.
Run this script on Databricks to benchmark ETL performance.
"""

import sys
from pathlib import Path

# Add benchmark module to path
sys.path.insert(0, str(Path(__file__).parent))

from benchmark.config import BenchmarkConfig, Platform, LoadType
from benchmark.runner import run_benchmark

# Example configuration for Databricks
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run TPC-DI benchmark on Databricks")
    parser.add_argument("--load-type", choices=["batch", "incremental"], required=True,
                       help="Type of load: batch or incremental")
    parser.add_argument("--scale-factor", type=int, required=True,
                       help="TPC-DI scale factor (e.g., 10, 100, 1000)")
    parser.add_argument("--output-path", default="dbfs:/mnt/tpcdi",
                       help="Raw data location: DBFS or Volume base path (default: dbfs:/mnt/tpcdi)")
    parser.add_argument("--cloud", choices=["AWS", "GCP", "Azure"],
                       help="Cloud provider (for informational purposes)")
    parser.add_argument("--target-database", default="tpcdi_warehouse",
                       help="Target database name (default: tpcdi_warehouse)")
    parser.add_argument("--target-schema", default="dw",
                       help="Target schema name (default: dw)")
    parser.add_argument("--target-catalog",
                       help="Unity Catalog name (optional); when set, create catalog + schema")
    parser.add_argument("--batch-id", type=int,
                       help="Batch ID for incremental loads (required for incremental)")
    parser.add_argument("--metrics-output", default="dbfs:/mnt/tpcdi/metrics",
                       help="Path to save metrics JSON (default: dbfs:/mnt/tpcdi/metrics)")
    parser.add_argument("--log-detailed-stats", action="store_true",
                       help="Log per-table timing and records; default is only job start/end/total duration")
    
    args = parser.parse_args()
    
    # Recommended instance types per cloud for TPC-DI ETL workload
    DEFAULT_NODE_TYPES = {
        "AWS": ("i3.xlarge", "i3.xlarge"),           # or i3.2xlarge for SF 100+
        "GCP": ("c2-standard-16", "c2-standard-16"), # or n2d-highmem-16, n2d-standard-16
        "Azure": ("Standard_E8s_v3", "Standard_E8s_v3"),  # or Standard_D8s_v3
    }
    
    if args.cloud:
        print(f"Cloud: {args.cloud} | Recommended: Worker/Driver = {DEFAULT_NODE_TYPES[args.cloud][0]}")
    
    # output_path = raw data base; runner appends /sf={scale_factor}
    config = BenchmarkConfig(
        platform=Platform.DATABRICKS,
        load_type=LoadType(args.load_type),
        scale_factor=args.scale_factor,
        raw_data_path=args.output_path,
        target_database=args.target_database,
        target_schema=args.target_schema,
        target_catalog=args.target_catalog,
        output_path=args.output_path,
        batch_id=args.batch_id,
        metrics_output_path=args.metrics_output,
        log_detailed_stats=args.log_detailed_stats,
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
