#!/usr/bin/env python3
"""
Unified wrapper script to run TPC-DI benchmarks from your laptop.
Supports submitting to Dataproc, Databricks, or running locally.

Usage:
  # Submit to Dataproc
  python run_benchmark.py dataproc --cluster my-cluster --load-type batch --scale-factor 10 ...

  # Submit to Databricks (workflow)
  python run_benchmark.py databricks --job-id 123 --load-type batch --scale-factor 10 ...

  # Run locally (requires Spark installed)
  python run_benchmark.py local --load-type batch --scale-factor 10 ...
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Dict, List, Optional


def ensure_benchmark_zip():
    """Create benchmark.zip if it doesn't exist."""
    zip_path = Path("benchmark.zip")
    if zip_path.exists():
        return str(zip_path)
    
    benchmark_dir = Path("benchmark")
    if not benchmark_dir.exists():
        print("ERROR: 'benchmark' directory not found. Run from project root.", file=sys.stderr)
        sys.exit(1)
    
    print(f"Creating {zip_path}...")
    shutil.make_archive("benchmark", "zip", ".", "benchmark")
    return str(zip_path)


def run_dataproc(args):
    """Submit benchmark to Dataproc cluster."""
    zip_path = ensure_benchmark_zip()
    
    # Build gcloud command
    cmd = [
        "gcloud", "dataproc", "jobs", "submit", "pyspark",
        "run_benchmark_dataproc.py",
        f"--cluster={args.cluster}",
        f"--region={args.region}",
        f"--project={args.project_id}",
        f"--py-files={zip_path}",
    ]
    
    # Add optional jars
    if args.jars:
        cmd.append(f"--jars={args.jars}")
    
    # Add benchmark arguments
    cmd.append("--")
    cmd.extend(["--load-type", args.load_type])
    cmd.extend(["--scale-factor", str(args.scale_factor)])
    cmd.extend(["--gcs-bucket", args.gcs_bucket])
    cmd.extend(["--project-id", args.project_id])
    cmd.extend(["--region", args.region])
    
    if args.raw_data_path:
        cmd.extend(["--raw-data-path", args.raw_data_path])
    if args.target_database:
        cmd.extend(["--target-database", args.target_database])
    if args.target_schema:
        cmd.extend(["--target-schema", args.target_schema])
    if args.batch_id:
        cmd.extend(["--batch-id", str(args.batch_id)])
    if args.spark_master:
        cmd.extend(["--spark-master", args.spark_master])
    if args.service_account_email:
        cmd.extend(["--service-account-email", args.service_account_email])
    if args.service_account_key_file:
        cmd.extend(["--service-account-key-file", args.service_account_key_file])
    if args.format:
        cmd.extend(["--format", args.format])
    if args.metrics_output:
        cmd.extend(["--metrics-output", args.metrics_output])
    if args.log_detailed_stats:
        cmd.append("--log-detailed-stats")
    if args.cluster_instance_type:
        cmd.extend(["--cluster-instance-type", args.cluster_instance_type])
    if args.cluster_worker_count:
        cmd.extend(["--cluster-worker-count", str(args.cluster_worker_count)])
    if args.cluster_master_type:
        cmd.extend(["--cluster-master-type", args.cluster_master_type])
    
    print(f"Submitting to Dataproc cluster: {args.cluster}")
    print(f"Command: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def run_databricks(args):
    """Submit benchmark to Databricks workflow."""
    if not args.job_id:
        print("ERROR: --job-id is required for Databricks workflow submission.", file=sys.stderr)
        sys.exit(1)
    
    # Build notebook params
    params = {
        "scale_factor": str(args.scale_factor),
        "load_type": args.load_type,
    }
    
    if args.output_path:
        params["tpcdi_raw_data_path"] = args.output_path
    if args.target_database:
        params["target_database"] = args.target_database
    if args.target_schema:
        params["target_schema"] = args.target_schema
    if args.target_catalog:
        params["target_catalog"] = args.target_catalog
    if args.batch_id:
        params["batch_id"] = str(args.batch_id)
    if args.metrics_output:
        params["metrics_output"] = args.metrics_output
    if args.log_detailed_stats:
        params["log_detailed_stats"] = "true"
    if args.cluster_instance_type:
        params["cluster_instance_type"] = args.cluster_instance_type
    if args.cluster_worker_count:
        params["cluster_worker_count"] = str(args.cluster_worker_count)
    if args.cluster_master_type:
        params["cluster_master_type"] = args.cluster_master_type
    
    # Use databricks CLI if available, else API
    try:
        subprocess.run(["databricks", "--version"], capture_output=True, check=True)
        use_cli = True
    except (subprocess.CalledProcessError, FileNotFoundError):
        use_cli = False
    
    if use_cli:
        # Use databricks CLI
        cmd = [
            "databricks", "jobs", "run-now",
            "--job-id", str(args.job_id),
            "--notebook-params", json.dumps(params),
        ]
        print(f"Submitting to Databricks job: {args.job_id}")
        print(f"Parameters: {json.dumps(params, indent=2)}")
        subprocess.run(cmd, check=True)
    else:
        # Use API (requires DATABRICKS_HOST and DATABRICKS_TOKEN env vars)
        host = os.environ.get("DATABRICKS_HOST")
        token = os.environ.get("DATABRICKS_TOKEN")
        
        if not host or not token:
            print(
                "ERROR: Databricks CLI not found. Either:\n"
                "  1. Install databricks-cli: pip install databricks-cli\n"
                "  2. Or set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables",
                file=sys.stderr,
            )
            sys.exit(1)
        
        import urllib.request
        import urllib.parse
        
        url = f"{host.rstrip('/')}/api/2.1/jobs/run-now"
        data = json.dumps({
            "job_id": int(args.job_id),
            "notebook_params": params,
        }).encode()
        
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        
        print(f"Submitting to Databricks job: {args.job_id} via API")
        print(f"Parameters: {json.dumps(params, indent=2)}")
        
        try:
            with urllib.request.urlopen(req) as resp:
                result = json.loads(resp.read())
                print(f"Run submitted. Run ID: {result.get('run_id')}")
        except Exception as e:
            print(f"ERROR: Failed to submit job: {e}", file=sys.stderr)
            sys.exit(1)


def run_local(args):
    """Run benchmark locally (requires Spark installed)."""
    import sys
    from pathlib import Path
    
    # Ensure benchmark is importable
    project_root = Path(__file__).parent
    sys.path.insert(0, str(project_root))
    
    try:
        from benchmark.config import BenchmarkConfig, Platform, LoadType
        from benchmark.runner import run_benchmark
    except ImportError as e:
        print(f"ERROR: Cannot import benchmark module: {e}", file=sys.stderr)
        print("Make sure you're running from the project root and dependencies are installed.", file=sys.stderr)
        sys.exit(1)
    
    # Determine platform from data path
    raw_path = args.raw_data_path or args.output_path or ""
    if raw_path.startswith("gs://"):
        platform = Platform.DATAPROC
        if not args.gcs_bucket:
            # Extract bucket from path
            bucket_match = __import__("re").match(r"gs://([^/]+)", raw_path)
            if bucket_match:
                gcs_bucket = bucket_match.group(1)
            else:
                print("ERROR: Cannot infer gcs_bucket from raw_data_path. Pass --gcs-bucket.", file=sys.stderr)
                sys.exit(1)
        else:
            gcs_bucket = args.gcs_bucket
    else:
        platform = Platform.DATABRICKS
    
    # Build config
    config_kwargs = {
        "platform": platform,
        "load_type": LoadType(args.load_type),
        "scale_factor": args.scale_factor,
        "raw_data_path": args.raw_data_path or args.output_path or ".",
        "target_database": args.target_database or "tpcdi_warehouse",
        "target_schema": args.target_schema or "dw",
        "batch_id": args.batch_id,
        "enable_metrics": True,
        "metrics_output_path": args.metrics_output or "./metrics",
        "log_detailed_stats": args.log_detailed_stats,
    }
    
    if platform == Platform.DATAPROC:
        config_kwargs.update({
            "gcs_bucket": gcs_bucket,
            "project_id": args.project_id or os.environ.get("GOOGLE_CLOUD_PROJECT"),
            "region": args.region or "us-central1",
            "service_account_email": args.service_account_email,
            "service_account_key_file": args.service_account_key_file,
            "table_format": args.format or "parquet",
        })
        if not config_kwargs["project_id"]:
            print("ERROR: --project-id required for Dataproc platform.", file=sys.stderr)
            sys.exit(1)
    else:
        config_kwargs.update({
            "output_path": args.output_path,
            "target_catalog": args.target_catalog,
        })
    
    if args.cluster_instance_type:
        config_kwargs["cluster_instance_type"] = args.cluster_instance_type
    if args.cluster_worker_count:
        config_kwargs["cluster_worker_count"] = args.cluster_worker_count
    if args.cluster_master_type:
        config_kwargs["cluster_master_type"] = args.cluster_master_type
    
    config = BenchmarkConfig(**config_kwargs)
    
    print(f"Running benchmark locally (platform: {platform.value})")
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


def main():
    parser = argparse.ArgumentParser(
        description="Unified wrapper to run TPC-DI benchmarks from your laptop",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Submit to Dataproc
  python run_benchmark.py dataproc --cluster my-cluster --load-type batch --scale-factor 10 \\
    --gcs-bucket my-bucket --project-id my-project --region us-central1

  # Submit to Databricks workflow
  python run_benchmark.py databricks --job-id 123 --load-type batch --scale-factor 10 \\
    --output-path dbfs:/mnt/tpcdi

  # Run locally
  python run_benchmark.py local --load-type batch --scale-factor 10 \\
    --raw-data-path ./data --metrics-output ./metrics
        """,
    )
    
    subparsers = parser.add_subparsers(dest="platform", help="Platform to run on")
    
    # Common arguments
    def add_common_args(p):
        p.add_argument("--load-type", choices=["batch", "incremental"], required=True,
                      help="Type of load: batch or incremental")
        p.add_argument("--scale-factor", type=int, required=True,
                      help="TPC-DI scale factor (e.g., 10, 100, 1000)")
        p.add_argument("--target-database", default="tpcdi_warehouse",
                      help="Target database name")
        p.add_argument("--target-schema", default="dw",
                      help="Target schema name")
        p.add_argument("--batch-id", type=int,
                      help="Batch ID for incremental loads")
        p.add_argument("--metrics-output",
                      help="Path to save metrics JSON")
        p.add_argument("--log-detailed-stats", action="store_true",
                      help="Log per-table timing and records")
        p.add_argument("--cluster-instance-type",
                      help="Worker instance type for metrics")
        p.add_argument("--cluster-worker-count", type=int,
                      help="Number of worker instances for metrics")
        p.add_argument("--cluster-master-type",
                      help="Driver/master instance type for metrics")
    
    # Dataproc subparser
    parser_dataproc = subparsers.add_parser("dataproc", help="Submit to Dataproc cluster")
    parser_dataproc.add_argument("--cluster", required=True,
                                 help="Dataproc cluster name")
    parser_dataproc.add_argument("--region", default="us-central1",
                                help="GCP region (default: us-central1)")
    parser_dataproc.add_argument("--project-id", required=True,
                                help="GCP project ID")
    parser_dataproc.add_argument("--gcs-bucket", required=True,
                                help="GCS bucket name")
    parser_dataproc.add_argument("--raw-data-path",
                                help="Base path to raw TPC-DI data in GCS (default: gs://<bucket>/tpcdi)")
    parser_dataproc.add_argument("--spark-master",
                                help="Spark master URL (default: yarn)")
    parser_dataproc.add_argument("--service-account-email",
                                help="Service account email for GCS access")
    parser_dataproc.add_argument("--service-account-key-file",
                                help="Path to service account JSON key file")
    parser_dataproc.add_argument("--format", choices=["delta", "parquet"], default="parquet",
                                 help="Table format (default: parquet)")
    parser_dataproc.add_argument("--jars",
                                help="Additional JAR files (comma-separated)")
    add_common_args(parser_dataproc)
    
    # Databricks subparser
    parser_databricks = subparsers.add_parser("databricks", help="Submit to Databricks workflow")
    parser_databricks.add_argument("--job-id", type=int,
                                  help="Databricks job/workflow ID (required)")
    parser_databricks.add_argument("--output-path",
                                   help="Raw data location: DBFS, Volume, or GCS path")
    parser_databricks.add_argument("--target-catalog",
                                   help="Unity Catalog name (optional)")
    add_common_args(parser_databricks)
    
    # Local subparser
    parser_local = subparsers.add_parser("local", help="Run locally (requires Spark)")
    parser_local.add_argument("--raw-data-path",
                             help="Path to raw TPC-DI data (local or gs://)")
    parser_local.add_argument("--output-path",
                             help="Output path (for Databricks platform)")
    parser_local.add_argument("--gcs-bucket",
                             help="GCS bucket (required if raw-data-path is gs://)")
    parser_local.add_argument("--project-id",
                             help="GCP project ID (required for Dataproc platform)")
    parser_local.add_argument("--region", default="us-central1",
                             help="GCP region (default: us-central1)")
    parser_local.add_argument("--service-account-email",
                             help="Service account email for GCS")
    parser_local.add_argument("--service-account-key-file",
                             help="Path to service account JSON key file")
    parser_local.add_argument("--format", choices=["delta", "parquet"], default="parquet",
                             help="Table format (default: parquet)")
    parser_local.add_argument("--target-catalog",
                             help="Unity Catalog name (for Databricks platform)")
    add_common_args(parser_local)
    
    args = parser.parse_args()
    
    if not args.platform:
        parser.print_help()
        sys.exit(1)
    
    if args.platform == "dataproc":
        run_dataproc(args)
    elif args.platform == "databricks":
        run_databricks(args)
    elif args.platform == "local":
        run_local(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
