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
    parser.add_argument("--load-type", choices=["batch", "incremental"],
                       help="Type of load: batch or incremental (required unless --list-node-types)")
    parser.add_argument("--scale-factor", type=int,
                       help="TPC-DI scale factor (e.g., 10, 100, 1000) (required unless --list-node-types)")
    parser.add_argument("--output-path", default="dbfs:/mnt/tpcdi",
                       help="Raw data location: DBFS or Volume base path (default: dbfs:/mnt/tpcdi)")
    parser.add_argument("--cloud", choices=["AWS", "GCP", "Azure"],
                       help="Cloud provider (used to show allowed instance types)")
    parser.add_argument("--list-node-types", action="store_true",
                       help="Print allowed instance types per cloud and exit (use with or without --cloud)")
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

    # Allowed instance types per cloud (must match create_workflow_notebook / create_databricks_workflow)
    CLOUD_NODE_OPTIONS = {
        "AWS": ["i3.xlarge", "i3.2xlarge", "i3.4xlarge", "m5d.xlarge", "m5d.2xlarge", "m5d.4xlarge", "r5d.xlarge", "r5d.2xlarge", "r5d.4xlarge"],
        "GCP": ["c2-standard-4", "c2-standard-8", "c2-standard-16", "c2-standard-30"] + [f"n2d-standard-{n}" for n in [4, 8, 16, 32, 48, 64, 80, 96]] + [f"n2d-highmem-{n}" for n in [4, 8, 16, 32, 48, 64, 80, 96]],
        "Azure": ["Standard_E4s_v3", "Standard_E8s_v3", "Standard_E16s_v3", "Standard_E32s_v3", "Standard_D4s_v3", "Standard_D8s_v3", "Standard_D16s_v3", "Standard_D32s_v3", "Standard_L4s_v2", "Standard_L8s_v2", "Standard_L16s_v2", "Standard_L32s_v2"],
    }
    DEFAULT_NODE_TYPES = {"AWS": ("i3.xlarge", "i3.xlarge"), "GCP": ("c2-standard-16", "c2-standard-16"), "Azure": ("Standard_E8s_v3", "Standard_E8s_v3")}

    if args.list_node_types:
        clouds = [args.cloud] if args.cloud else ["AWS", "GCP", "Azure"]
        for c in clouds:
            opts = CLOUD_NODE_OPTIONS[c]
            default = DEFAULT_NODE_TYPES[c][0]
            print(f"{c}: {', '.join(opts)} (default: {default})")
        return 0

    if not args.load_type or args.scale_factor is None:
        parser.error("--load-type and --scale-factor are required (unless using --list-node-types)")
    if args.cloud:
        print(f"Cloud: {args.cloud} | Recommended: Worker/Driver = {DEFAULT_NODE_TYPES[args.cloud][0]} | Allowed: {', '.join(CLOUD_NODE_OPTIONS[args.cloud])}")
    
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
