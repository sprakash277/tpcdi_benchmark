"""
TPC-DI v2: Stats gathering and benchmark report for run_tpcdi_batch.
All stateless helpers; run_tpcdi_batch holds steps/table_details and calls these.
"""

from typing import List, Dict, Any, Optional, Tuple


def sql_file_to_table_name(rel_path: str) -> Optional[str]:
    """Map SQL file path to short table name (e.g. sql/bronze/load_bronze_date.sql -> bronze_date)."""
    base = rel_path.replace("\\", "/").split("/")[-1].replace(".sql", "")
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
    if base.startswith("load_bronze_incremental_"):
        return "bronze_" + base[len("load_bronze_incremental_"):]
    return None


def get_table_stats(spark, catalog: str, schema_name: str, table_short_name: str) -> Tuple[int, float]:
    """Return (row_count, size_mb) for catalog.schema.table_short_name. Returns (0, 0) if table missing or error."""
    full = f"{catalog}.{schema_name}.{table_short_name}"
    try:
        if not spark.catalog.tableExists(full):
            return 0, 0.0
        row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {full}").collect()[0]["cnt"]
        detail = spark.sql(f"DESCRIBE DETAIL {full}").collect()[0]
        size_bytes = detail.get("sizeInBytes") or 0
        size_mb = size_bytes / (1024 * 1024) if size_bytes else 0.0
        return row_count, size_mb
    except Exception:
        return 0, 0.0


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
        "",
    ]

    try:
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
            cb = cost.get("cost_breakdown") or {}
            total_cost = cost.get("total_cost_usd")
            dbu_cost = cost.get("dbu_cost_usd")
            lines.append("Cost (estimated):")
            if cb.get("compute_usd") is not None:
                lines.append(f"  Compute: ${cb['compute_usd']:.2f}")
            if cb.get("software_usd") is not None:
                lines.append(f"  Software: ${cb['software_usd']:.2f}")
            if total_cost is not None:
                lines.append(f"  Total cost: ${total_cost:.2f}")
            if dbu_cost is not None:
                lines.append(f"  DBU cost: ${dbu_cost:.2f}")
            lines.append("")
    except Exception:
        lines.append("Cost (estimated): N/A (benchmark.cost not available)")
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
