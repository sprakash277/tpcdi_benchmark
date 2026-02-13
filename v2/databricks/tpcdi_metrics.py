"""
TPC-DI v2: Stats gathering and benchmark report for run_tpcdi_batch.
All stateless helpers; run_tpcdi_batch holds steps/table_details and calls these.
Metrics output: save metrics JSON to GCS, dbfs:, or local path (v1-style).
"""

import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple


def _write_string_to_gcs_via_spark(spark, gcs_path: str, content: str) -> bool:
    """Write a string to a GCS path using Spark's Hadoop FileSystem (Databricks with GCS connector)."""
    try:
        sc = spark.sparkContext
        jvm = sc._jvm
        hadoop_conf = sc._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI.create(gcs_path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        path = jvm.org.apache.hadoop.fs.Path(gcs_path)
        out = fs.create(path, True)
        content_bytes = content.encode("utf-8")
        out.write(content_bytes)
        out.close()
        fs.close()
        return True
    except Exception:
        return False


def sql_file_to_table_name(rel_path: str) -> Optional[str]:
    """Map SQL file path to short table name (e.g. sql/bronze/load_bronze_date.sql -> bronze_date)."""
    base = rel_path.replace("\\", "/").split("/")[-1].replace(".sql", "")
    # More specific patterns first (incremental) so they are not matched by generic load_bronze_ / load_gold_ / transform_silver_
    if base.startswith("load_bronze_incremental_"):
        return "bronze_" + base[len("load_bronze_incremental_"):]
    if base.startswith("load_bronze_"):
        return "bronze_" + base[len("load_bronze_"):]
    if base.startswith("load_gold_incremental_"):
        return "gold_" + base[len("load_gold_incremental_"):]
    if base.startswith("load_gold_"):
        return "gold_" + base[len("load_gold_"):]
    if base.startswith("transform_silver_incremental_"):
        return "silver_" + base[len("transform_silver_incremental_"):]
    if base.startswith("transform_silver_"):
        return "silver_" + base[len("transform_silver_"):]
    return None


def get_table_stats(
    spark,
    catalog: str,
    schema_name: str,
    table_short_name: str,
    use_refresh: bool = False,
) -> Tuple[int, float, float]:
    """Return (row_count, size_mb, refresh_seconds) for catalog.schema.table_short_name.
    When use_refresh=True (batch), switch to catalog/schema and run REFRESH TABLE so COUNT/DESCRIBE see current state.
    When use_refresh=False (incremental), refresh_seconds is 0. Returns (0, 0.0, 0.0) if table missing or error."""
    full = f"{catalog}.{schema_name}.{table_short_name}"
    refresh_seconds = 0.0
    try:
        if not spark.catalog.tableExists(full):
            print(f"[get_table_stats] Table not found: {full}")
            return 0, 0.0, 0.0
        if use_refresh:
            import time as _time
            t0 = _time.time()
            spark.sql(f"USE CATALOG {catalog}")
            spark.sql(f"USE SCHEMA {schema_name}")
            try:
                spark.sql(f"REFRESH TABLE `{table_short_name}`")
            except Exception:
                try:
                    spark.catalog.refreshTable(full)
                except Exception:
                    pass
            refresh_seconds = _time.time() - t0
        row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full}").collect()[0]["cnt"]
        detail = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]
        size_bytes = 0
        try:
            size_bytes = detail["sizeInBytes"]
        except (KeyError, TypeError, AttributeError):
            size_bytes = getattr(detail, "sizeInBytes", 0)
        size_bytes = size_bytes or 0
        size_mb = size_bytes / (1024 * 1024) if size_bytes else 0.0
        return row_count, size_mb, refresh_seconds
    except Exception as e:
        print(f"[get_table_stats] {full}: {type(e).__name__}: {e}")
        return 0, 0.0, 0.0


def record_table_load(
    table_details: List[Dict[str, Any]],
    table_key: str,
    duration_seconds: float,
    row_count: int,
    size_mb: float,
    catalog: str,
    schema_name: str,
) -> None:
    """Append one entry to table_details. table_key can be 'gold_dim_customer' or 'optimize:gold_dim_company'."""
    full = f"{catalog}.{schema_name}.{table_key}" if ":" not in table_key else table_key
    bytes_processed = int(size_mb * 1024 * 1024) if size_mb else 0
    table_details.append({
        "table": full,
        "duration_seconds": duration_seconds,
        "row_count": row_count,
        "bytes_processed": bytes_processed,
    })


def print_benchmark_report(
    spark,
    steps: List[Dict[str, Any]],
    table_details: List[Dict[str, Any]],
    job_start_time: Optional[float],
    job_end_time: Optional[float],
    catalog: str,
    schema_name: str,
    load_type: str,
    sf: str,
    total_refresh_seconds: float = 0.0,
) -> None:
    """Print TPC-DI benchmark results in V1 format (steps, table-level stats, optional cost)."""
    total_duration = (job_end_time - job_start_time) if job_end_time and job_start_time else 0
    total_rows = sum(d["row_count"] for d in table_details)
    total_bytes = sum(d.get("bytes_processed") or 0 for d in table_details)
    total_mb = total_bytes / (1024 * 1024) if total_bytes else 0
    rows_per_sec = total_rows / total_duration if total_duration > 0 else 0
    mb_per_sec = total_mb / total_duration if total_duration > 0 else 0
    completed = sum(1 for s in steps if s.get("duration_seconds", 0) >= 0)
    failed = 0

    try:
        worker_type = spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType", "N/A")
        driver_type = spark.conf.get("spark.databricks.clusterUsageTags.clusterDriverNodeType", worker_type)
        num_workers_str = spark.conf.get("spark.databricks.clusterUsageTags.clusterWorkers", "")
        num_workers = int(num_workers_str) if num_workers_str else "N/A"
    except Exception:
        worker_type = driver_type = "N/A"
        num_workers = "N/A"

    sep = "=" * 80
    lines = [
        "",
        sep,
        "TPC-DI BENCHMARK RESULTS - DATABRICKS",
        sep,
        "Platform: databricks",
        "Compute: classic",
        f"Load Type: {load_type}",
        f"Scale Factor: {sf}",
        "",
        "Cluster Configuration:",
        f"  Worker Node Type: {worker_type}",
        f"  Driver Node Type: {driver_type}",
        f"  Number of Worker Nodes: {num_workers}",
        "",
        "Table Override: True",
        "",
        f"Total Duration: {total_duration:.2f} seconds",
        "",
        "Summary:",
        f"  Total Steps: {len(steps)}",
        f"  Completed Steps: {completed}",
        f"  Failed Steps: {failed}",
        f"  Total Rows Processed: {total_rows:,}",
        f"  Total Data Size: {total_mb:.2f} MB",
        f"  Throughput: {rows_per_sec:.2f} rows/sec",
        f"  Data Throughput: {mb_per_sec:.2f} MB/sec",
    ]
    if total_refresh_seconds > 0:
        lines.append(f"  Table stats refresh: {total_refresh_seconds:.2f}s")
    lines.append("")

    try:
        try:
            from cost import estimate_databricks_cost
        except ImportError:
            from benchmark.cost import estimate_databricks_cost
        cost = estimate_databricks_cost(
            total_duration_seconds=total_duration,
            cluster_worker_count=num_workers if isinstance(num_workers, int) else 4,
            cluster_instance_type=worker_type if worker_type != "N/A" else None,
            cluster_master_type=driver_type if driver_type != "N/A" else None,
            databricks_compute_type="classic",
            cloud="GCP",
        )
        if cost:
            lines.append("Cost (estimated):")
            compute_usd = cost.get("compute_usd")
            software_usd = cost.get("software_usd")
            total_usd = cost.get("total_usd")
            dbu_usd = cost.get("dbu_usd")
            if compute_usd is not None:
                lines.append(f"  Compute: ${compute_usd:.2f}")
            if software_usd is not None:
                lines.append(f"  Software: ${software_usd:.2f}")
            if total_usd is not None:
                lines.append(f"  Total cost: ${total_usd:.2f}")
            if dbu_usd is not None:
                lines.append(f"  DBU cost: ${dbu_usd:.2f}")
            lines.append("")
    except Exception:
        lines.append("Cost (estimated): Use Databricks usage / billable DBU for cost.")
        lines.append("")

    lines.append("Step Details:")
    for s in steps:
        name = s.get("step_name", "?")
        dur = s.get("duration_seconds", 0)
        rows = s.get("rows_processed", 0)
        icon = "✓"
        lines.append(f"  {icon} {name}: {dur:.2f}s" + (f" ({rows:,} rows)" if rows else ""))
    lines.append("")

    lines.append("Table-level stats:")
    lines.append(f"  Tables loaded:      {len(table_details)}")
    lines.append(f"  Total records:      {total_rows:,}")
    lines.append(f"  Total data size:    {total_mb:.2f} MB")
    lines.append(f"  Overall throughput: {rows_per_sec:,.1f} rows/s, {mb_per_sec:.2f} MB/s")
    lines.append("  Per-table (duration, rows, size, throughput):")
    for d in table_details:
        tbl = d.get("table", "?")
        dur = d.get("duration_seconds") or 0
        rows = d.get("row_count") or 0
        b = d.get("bytes_processed") or 0
        size_mb = b / (1024 * 1024) if b else 0
        row_s = rows / dur if dur > 0 else 0
        mb_s = size_mb / dur if dur > 0 else 0
        lines.append(f"    - {tbl}: {dur:.2f}s, {rows:,} rows, {size_mb:.2f} MB, {row_s:,.1f} rows/s, {mb_s:.2f} MB/s")
    lines.append(sep)
    print("\n".join(lines))


def save_metrics_output(
    spark,
    steps: List[Dict[str, Any]],
    table_details: List[Dict[str, Any]],
    job_start_time: float,
    job_end_time: float,
    catalog: str,
    schema_name: str,
    load_type: str,
    sf: str,
    metrics_output_path: str,
    batch_id: Optional[str] = None,
    total_refresh_seconds: float = 0.0,
    cost_dict: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """
    Save metrics to a JSON file (v1-style). Default path gs://sumit_prakash_gcs/tpcdi/metrics.
    For gs://: tries gsutil then Spark GCS. For dbfs:/ uses /dbfs mount. For local: creates directory and writes file.
    Returns the path written, or None on failure.
    """
    if not metrics_output_path or not metrics_output_path.strip():
        return None
    path = metrics_output_path.strip().rstrip("/")
    start_time = job_start_time
    total_duration = (job_end_time - job_start_time) if job_end_time and job_start_time else 0
    total_rows = sum(d.get("row_count") or 0 for d in table_details)
    total_bytes = sum(d.get("bytes_processed") or 0 for d in table_details)
    total_mb = total_bytes / (1024 * 1024) if total_bytes else 0

    timestamp = datetime.fromtimestamp(start_time).strftime("%Y%m%d_%H%M%S")
    filename = f"metrics_databricks_{load_type}_sf{sf}_{timestamp}.json"
    if batch_id:
        filename = f"metrics_databricks_{load_type}_sf{sf}_batch{batch_id}_{timestamp}.json"

    payload = {
        "platform": "databricks",
        "load_type": load_type,
        "scale_factor": int(sf) if str(sf).isdigit() else sf,
        "batch_id": int(batch_id) if batch_id and str(batch_id).isdigit() else batch_id,
        "catalog": catalog,
        "schema_name": schema_name,
        "start_time": start_time,
        "start_time_iso": datetime.fromtimestamp(start_time).isoformat(),
        "end_time": job_end_time,
        "end_time_iso": datetime.fromtimestamp(job_end_time).isoformat() if job_end_time else None,
        "total_duration_seconds": total_duration,
        "total_rows_processed": total_rows,
        "total_data_mb": round(total_mb, 4),
        "total_refresh_seconds": total_refresh_seconds,
        "steps": steps,
        "table_details": table_details,
    }
    if cost_dict:
        payload["cost_breakdown"] = cost_dict
        payload["total_cost_usd"] = cost_dict.get("total_usd")

    json_content = json.dumps(payload, indent=2)

    if path.startswith("gs://"):
        full_gcs_path = f"{path}/{filename}"
        gsutil_cmd = shutil.which("gsutil")
        if gsutil_cmd:
            try:
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                    f.write(json_content)
                    tmp_path = f.name
                subprocess.run([gsutil_cmd, "-q", "cp", tmp_path, full_gcs_path], check=True, capture_output=True)
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                print(f"Metrics saved to {full_gcs_path}")
                return full_gcs_path
            except (subprocess.CalledProcessError, FileNotFoundError) as e:
                print(f"gsutil upload failed: {e}, trying Spark GCS write")
        if _write_string_to_gcs_via_spark(spark, full_gcs_path, json_content):
            print(f"Metrics saved to {full_gcs_path} (via Spark GCS)")
            return full_gcs_path
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(json_content)
            tmp_path = f.name
        print(f"WARN: Could not upload to GCS. Metrics written to {tmp_path}")
        return tmp_path

    # dbfs:/ or local path
    if path.startswith("dbfs:/"):
        out_dir = Path("/dbfs" + path[4:])  # /dbfs/FileStore/... or /dbfs/mnt/...
    else:
        out_dir = Path(path)
    out_dir.mkdir(parents=True, exist_ok=True)
    filepath = out_dir / filename
    with open(filepath, "w") as f:
        f.write(json_content)
    print(f"Metrics saved to {filepath}")
    return str(filepath)
