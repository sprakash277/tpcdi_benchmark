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
    sys.path.insert(0, str(script_dir))
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
                       help="Base path to raw TPC-DI data in GCS; /sf=<scale_factor> is appended (default: gs://<bucket>/tpcdi)")
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
    parser.add_argument("--log-detailed-stats", nargs="?", default=False, const=True, metavar="true|false",
                       help="Log per-table timing, bytes, and throughput (default: false). Use --log-detailed-stats or --log-detailed-stats true to enable.")
    parser.add_argument("--format", choices=["delta", "parquet"], default="parquet",
                       help="Table format for warehouse tables (default: parquet). Use delta only if Delta package is on cluster (e.g. --packages io.delta:delta-spark_2.12:3.0.0).")
    
    args = parser.parse_args()

    # Normalize --log-detailed-stats (accepts no value, true, or false)
    if args.log_detailed_stats is True:
        pass  # flag given with no value -> True
    elif isinstance(args.log_detailed_stats, str):
        args.log_detailed_stats = args.log_detailed_stats.strip().lower() in ("true", "yes", "1")

    # Construct raw data path (base only; runner appends /sf={scale_factor} like Databricks)
    if args.raw_data_path:
        raw_data_path = args.raw_data_path.rstrip("/")
    else:
        raw_data_path = f"gs://{args.gcs_bucket}/tpcdi"
    
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

    # Same summary format as Databricks (benchmark_databricks_notebook.py)
    print("\n" + "=" * 80)
    print("TPC-DI BENCHMARK RESULTS - DATAPROC")
    print("=" * 80)
    print(f"Platform: {result['config']['platform']}")
    print(f"Load Type: {result['config']['load_type']}")
    print(f"Scale Factor: {result['config']['scale_factor']}")
    if result['config'].get('batch_id'):
        print(f"Batch ID: {result['config']['batch_id']}")
    total_dur = result['metrics'].get('total_duration_seconds')
    print(f"\nTotal Duration: {total_dur:.2f} seconds" if total_dur is not None else "\nTotal Duration: N/A")
    summary = result['metrics'].get('summary')
    if summary:
        print("\nSummary:")
        print(f"  Total Steps: {summary.get('total_steps', 0)}")
        print(f"  Completed Steps: {summary.get('completed_steps', 0)}")
        print(f"  Failed Steps: {summary.get('failed_steps', 0)}")
        print(f"  Total Rows Processed: {summary.get('total_rows_processed', 0):,}")
        total_bytes = summary.get('total_bytes_processed') or 0
        print(f"  Total Data Size: {total_bytes / (1024 * 1024):.2f} MB")
        print(f"  Throughput: {summary.get('throughput_rows_per_second', 0):.2f} rows/sec")
        print(f"  Data Throughput: {summary.get('throughput_mb_per_second', 0):.2f} MB/sec")

    print("\nStep Details:")
    for step in result['metrics'].get('steps', []):
        status_icon = "✓" if step.get('status') == "completed" else "✗" if step.get('status') == "failed" else "○"
        dur = step.get('duration_seconds')
        dur_str = f"{dur:.2f}s" if dur is not None else "N/A"
        print(f"  {status_icon} {step.get('step_name', '?')}: {dur_str}", end="")
        if step.get('rows_processed') is not None:
            print(f" ({step['rows_processed']:,} rows)", end="")
        if step.get('status') == "failed" and step.get('error_message'):
            print(f" - ERROR: {step['error_message']}", end="")
        print()

    # Table-level stats in Result summary only when --log-detailed-stats is true (same as Databricks)
    if args.log_detailed_stats:
        try:
            from benchmark.etl.table_timing import get_summary as get_table_summary
            tsum = get_table_summary()
            details = tsum.get("table_details") or []
            if details:
                total_rows = tsum.get("total_records_loaded") or 0
                total_bytes = tsum.get("total_bytes_processed") or 0
                total_dur = tsum.get("total_duration_seconds") or 0
                total_mb = total_bytes / (1024 * 1024)
                rows_per_sec = total_rows / total_dur if total_dur > 0 else 0
                mb_per_sec = total_mb / total_dur if total_dur > 0 and total_bytes else 0
                print("\nTable-level stats:")
                print(f"  Tables loaded:      {len(details)}")
                print(f"  Total records:      {total_rows:,}")
                print(f"  Total data size:    {total_mb:.2f} MB")
                print(f"  Overall throughput: {rows_per_sec:,.1f} rows/s, {mb_per_sec:.2f} MB/s")
                print("  Per-table (duration, rows, size, throughput):")
                for d in details:
                    dur = d.get("duration_seconds") or 0
                    rows = d.get("row_count") or 0
                    b = d.get("bytes_processed")
                    row_s = rows / dur if dur > 0 else 0
                    mb_s = (b / (1024 * 1024)) / dur if b and dur > 0 else None
                    size_str = f", {b / (1024 * 1024):.2f} MB" if b else ""
                    tp_str = f", {row_s:,.1f} rows/s" + (f", {mb_s:.2f} MB/s" if mb_s is not None else "")
                    print(f"    - {d.get('table', '?')}: {dur:.2f}s, {rows:,} rows{size_str}{tp_str}")
        except Exception as e:
            print(f"\n  (Table-level stats unavailable: {e})")

    print("=" * 80)
