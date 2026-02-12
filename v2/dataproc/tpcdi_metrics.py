"""
TPC-DI v2 Dataproc: Stats and benchmark report for run_tpcdi_batch.
Uses database.table (Hive metastore), Delta tables; no dbutils.
"""

from typing import List, Dict, Any, Optional, Tuple
import re


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
    database: str,
    table_short_name: str,
    use_refresh: bool = False,
) -> Tuple[int, float, float]:
    """Return (row_count, size_mb, refresh_seconds) for database.table_short_name (Delta)."""
    full = f"{database}.{table_short_name}"
    refresh_seconds = 0.0
    try:
        if not spark.catalog.tableExists(full):
            print(f"[get_table_stats] Table not found: {full}")
            return 0, 0.0, 0.0
        if use_refresh:
            import time as _time
            t0 = _time.time()
            spark.sql(f"REFRESH TABLE {full}")
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
    database: str,
) -> None:
    """Append one entry to table_details."""
    full = f"{database}.{table_key}" if ":" not in table_key else table_key
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
    database: str,
    load_type: str,
    sf: str,
    total_refresh_seconds: float = 0.0,
) -> None:
    """Print TPC-DI benchmark results for Dataproc (Delta)."""
    total_duration = (job_end_time - job_start_time) if job_end_time and job_start_time else 0
    total_rows = sum(d["row_count"] for d in table_details)
    total_bytes = sum(d.get("bytes_processed") or 0 for d in table_details)
    total_mb = total_bytes / (1024 * 1024) if total_bytes else 0
    rows_per_sec = total_rows / total_duration if total_duration > 0 else 0
    mb_per_sec = total_mb / total_duration if total_duration > 0 else 0
    completed = sum(1 for s in steps if s.get("duration_seconds", 0) >= 0)
    failed = 0

    sep = "=" * 80
    lines = [
        "",
        sep,
        "TPC-DI BENCHMARK RESULTS - DATAPROC (Delta)",
        sep,
        "Platform: dataproc",
        "Table format: delta",
        f"Load Type: {load_type}",
        f"Scale Factor: {sf}",
        f"Database: {database}",
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
    lines.append("Cost (estimated): N/A (Dataproc - use GCP billing for cost)")
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
