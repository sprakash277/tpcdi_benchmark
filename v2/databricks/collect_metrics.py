#!/usr/bin/env python3
"""
Collect and format metrics for TPC-DI v2 SQL implementation.

This script queries tables and generates a formatted report similar to v1 metrics output.
"""

import json
import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from pathlib import Path


def format_number(num: float, decimals: int = 2) -> str:
    """Format number with commas and decimals."""
    return f"{num:,.{decimals}f}"


def format_duration(seconds: float) -> str:
    """Format duration in seconds."""
    return f"{seconds:.2f}s"


def get_table_stats(spark, catalog: str, schema: str, table_name: str) -> Dict[str, Any]:
    """Get statistics for a table."""
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        # Get row count
        row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}").collect()[0]["cnt"]
        
        # Get table size (if available)
        try:
            size_info = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()[0]
            size_bytes = size_info.get("sizeInBytes", 0)
            size_mb = size_bytes / (1024 * 1024) if size_bytes else 0
        except Exception:
            size_mb = 0
        
        return {
            "table": full_table_name,
            "row_count": row_count,
            "size_mb": size_mb,
            "exists": True
        }
    except Exception as e:
        return {
            "table": full_table_name,
            "row_count": 0,
            "size_mb": 0,
            "exists": False,
            "error": str(e)
        }


def collect_v2_metrics(
    spark,
    catalog: str,
    bronze_schema: str,
    silver_schema: str,
    gold_schema: str,
    platform: str = "databricks",
    compute_type: str = "classic",
    load_type: str = "batch",
    scale_factor: int = 10,
    batch_id: Optional[int] = None,
    cluster_config: Optional[Dict[str, Any]] = None,
    start_time: Optional[float] = None,
    end_time: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Collect metrics from v2 SQL implementation.
    
    Returns a dictionary with all metrics formatted for reporting.
    """
    
    if start_time is None:
        start_time = time.time()
    if end_time is None:
        end_time = time.time()
    
    total_duration = end_time - start_time
    
    # Define all tables by layer
    bronze_tables = [
        "bronze_date", "bronze_time", "bronze_status_type", "bronze_tax_rate",
        "bronze_trade_type", "bronze_industry", "bronze_hr", "bronze_customer_mgmt",
        "bronze_customer", "bronze_account", "bronze_trade", "bronze_daily_market",
        "bronze_prospect", "bronze_cash_transaction", "bronze_holding_history",
        "bronze_watch_history", "bronze_finwire",
    ]
    
    silver_tables = [
        "silver_date", "silver_time", "silver_status_type", "silver_trade_type",
        "silver_industry", "silver_tax_rate", "silver_companies", "silver_securities",
        "silver_financials", "silver_customers", "silver_accounts", "silver_trades",
        "silver_daily_market", "silver_prospect", "silver_cash_transaction",
        "silver_watch_history", "silver_holding_history",
    ]
    
    gold_tables = [
        "gold_dim_date", "gold_dim_customer", "gold_dim_account", "gold_dim_company",
        "gold_dim_security", "gold_dim_trade_type", "gold_dim_status_type",
        "gold_dim_industry", "gold_dim_broker", "gold_dim_trade",
        "gold_fact_trade", "gold_fact_market_history", "gold_fact_cash_balances",
        "gold_fact_holdings", "gold_fact_watches", "gold_financials", "gold_prospect",
        "gold_dim_messages",
    ]
    
    # Collect stats for all tables
    table_stats = []
    total_rows = 0
    total_size_mb = 0
    
    print("Collecting table statistics...")
    
    # Bronze tables
    for table in bronze_tables:
        stats = get_table_stats(spark, catalog, bronze_schema, table)
        if stats["exists"]:
            table_stats.append({
                **stats,
                "layer": "bronze",
                "duration_seconds": 0,  # Would need to track from workflow
            })
            total_rows += stats["row_count"]
            total_size_mb += stats["size_mb"]
    
    # Silver tables
    for table in silver_tables:
        stats = get_table_stats(spark, catalog, silver_schema, table)
        if stats["exists"]:
            table_stats.append({
                **stats,
                "layer": "silver",
                "duration_seconds": 0,
            })
            total_rows += stats["row_count"]
            total_size_mb += stats["size_mb"]
    
    # Gold tables
    for table in gold_tables:
        stats = get_table_stats(spark, catalog, gold_schema, table)
        if stats["exists"]:
            table_stats.append({
                **stats,
                "layer": "gold",
                "duration_seconds": 0,
            })
            total_rows += stats["row_count"]
            total_size_mb += stats["size_mb"]
    
    # Calculate throughput
    throughput_rows_per_sec = total_rows / total_duration if total_duration > 0 else 0
    throughput_mb_per_sec = total_size_mb / total_duration if total_duration > 0 else 0
    
    # Build metrics dictionary
    metrics = {
        "platform": platform,
        "compute_type": compute_type,
        "load_type": load_type,
        "scale_factor": scale_factor,
        "batch_id": batch_id,
        "start_time": start_time,
        "end_time": end_time,
        "total_duration_seconds": total_duration,
        "cluster_config": cluster_config or {},
        "table_stats": table_stats,
        "summary": {
            "total_tables": len(table_stats),
            "total_rows": total_rows,
            "total_size_mb": total_size_mb,
            "throughput_rows_per_sec": throughput_rows_per_sec,
            "throughput_mb_per_sec": throughput_mb_per_sec,
        }
    }
    
    return metrics


def format_metrics_report(metrics: Dict[str, Any]) -> str:
    """Format metrics as a human-readable report."""
    
    platform = metrics["platform"]
    compute_type = metrics["compute_type"]
    load_type = metrics["load_type"]
    scale_factor = metrics["scale_factor"]
    batch_id = metrics.get("batch_id")
    total_duration = metrics["total_duration_seconds"]
    cluster_config = metrics.get("cluster_config", {})
    summary = metrics["summary"]
    table_stats = metrics["table_stats"]
    
    # Build report
    report_lines = [
        "=" * 80,
        "TPC-DI BENCHMARK RESULTS - DATABRICKS",
        "=" * 80,
        f"Platform: {platform}",
        f"Compute: {compute_type}",
        f"Load Type: {load_type}",
        f"Scale Factor: {scale_factor}",
        "",
    ]
    
    # Cluster configuration
    if cluster_config:
        report_lines.extend([
            "Cluster Configuration:",
            f"  Worker Node Type: {cluster_config.get('node_type_id', 'N/A')}",
            f"  Driver Node Type: {cluster_config.get('driver_node_type_id', 'N/A')}",
            f"  Number of Worker Nodes: {cluster_config.get('num_workers', 'N/A')}",
            "",
        ])
    
    # Summary
    report_lines.extend([
        f"Total Duration: {format_duration(total_duration)}",
        "",
        "Summary:",
        f"  Total Tables: {summary['total_tables']}",
        f"  Total Rows Processed: {format_number(summary['total_rows'], 0)}",
        f"  Total Data Size: {format_number(summary['total_size_mb'], 2)} MB",
        f"  Throughput: {format_number(summary['throughput_rows_per_sec'], 2)} rows/sec",
        f"  Data Throughput: {format_number(summary['throughput_mb_per_sec'], 2)} MB/sec",
        "",
    ])
    
    # Table-level stats
    report_lines.extend([
        "Table-level stats:",
        f"  Tables loaded:      {summary['total_tables']}",
        f"  Total records:      {format_number(summary['total_rows'], 0)}",
        f"  Total data size:    {format_number(summary['total_size_mb'], 2)} MB",
        f"  Overall throughput: {format_number(summary['throughput_rows_per_sec'], 1)} rows/s, {format_number(summary['throughput_mb_per_sec'], 2)} MB/s",
        "  Per-table (duration, rows, size, throughput):",
    ])
    
    # Sort tables by layer, then by name
    sorted_tables = sorted(table_stats, key=lambda x: (x["layer"], x["table"]))
    
    for stats in sorted_tables:
        table_name = stats["table"]
        row_count = stats["row_count"]
        size_mb = stats["size_mb"]
        duration = stats.get("duration_seconds", 0)
        
        rows_per_sec = row_count / duration if duration > 0 else 0
        mb_per_sec = size_mb / duration if duration > 0 else 0
        
        report_lines.append(
            f"    - {table_name}: {format_duration(duration)}, {format_number(row_count, 0)} rows, "
            f"{format_number(size_mb, 2)} MB, {format_number(rows_per_sec, 1)} rows/s, {format_number(mb_per_sec, 2)} MB/s"
        )
    
    report_lines.append("=" * 80)
    
    return "\n".join(report_lines)


def main():
    """Main function for command-line usage."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Collect TPC-DI v2 metrics")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--bronze-schema", required=True, help="Bronze schema")
    parser.add_argument("--silver-schema", required=True, help="Silver schema")
    parser.add_argument("--gold-schema", required=True, help="Gold schema")
    parser.add_argument("--platform", default="databricks", help="Platform name")
    parser.add_argument("--compute-type", default="classic", help="Compute type")
    parser.add_argument("--load-type", default="batch", help="Load type")
    parser.add_argument("--scale-factor", type=int, default=10, help="Scale factor")
    parser.add_argument("--batch-id", type=int, help="Batch ID")
    parser.add_argument("--output", help="Output JSON file")
    parser.add_argument("--report", action="store_true", help="Print formatted report")
    
    args = parser.parse_args()
    
    # Note: This would need to be run in a Databricks environment with Spark
    print("This script should be run in a Databricks notebook or environment with Spark")
    print("See v2/databricks/collect_metrics_notebook.py for notebook version")


if __name__ == "__main__":
    main()
