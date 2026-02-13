#!/usr/bin/env python3
"""
TPC-DI v3 Dataproc runner: PySpark implementation using v2 table/column names.
Bronze → Silver → Gold in PySpark (no SQL files). Same CLI and metrics as v2.
"""

import argparse
import os
import sys
import time
from pathlib import Path

# Add v2/dataproc for tpcdi_metrics
SCRIPT_DIR = Path(__file__).resolve().parent
V2_DATAPROC = (SCRIPT_DIR.parent.parent / "v2" / "dataproc").resolve()
if str(V2_DATAPROC) not in sys.path:
    sys.path.insert(0, str(V2_DATAPROC))

# v3 etl (same package as this script)
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from etl.bronze import load_bronze_batch, BRONZE_BATCH_PATHS
from etl.silver import transform_silver_batch
from etl.gold import load_gold_batch, GOLD_TABLE_ORDER

# Bronze load order (v2 order: ref + customer_mgmt/finwire via v2 scripts, then pipe-delimited)
BRONZE_ORDER = [
    "bronze_date", "bronze_time", "bronze_status_type", "bronze_trade_type", "bronze_industry", "bronze_tax_rate",
    "bronze_trade", "bronze_daily_market", "bronze_cash_transaction", "bronze_holding_history",
    "bronze_watch_history", "bronze_hr", "bronze_prospect",
]

SILVER_ORDER = [
    "silver_date", "silver_time", "silver_status_type", "silver_trade_type", "silver_industry", "silver_tax_rate",
    "silver_companies", "silver_securities", "silver_financials",
    "silver_trades", "silver_daily_market", "silver_cash_transaction", "silver_holding_history", "silver_watch_history",
    "silver_prospect",
]


def main():
    parser = argparse.ArgumentParser(description="TPC-DI v3 Dataproc: PySpark batch (v2 schema)")
    parser.add_argument("--database", default="tpcdi_dw")
    parser.add_argument("--raw-data-path", required=True)
    parser.add_argument("--sf", type=int, default=10)
    parser.add_argument("--batch-id", type=int, default=1)
    parser.add_argument("--load-type", choices=["batch", "incremental"], default="batch")
    parser.add_argument("--metrics-output", default="")
    args = parser.parse_args()

    database = args.database
    if not database.endswith(f"_sf{args.sf}"):
        database = f"{database}_sf{args.sf}"
    raw_data_path = args.raw_data_path.rstrip("/")
    full_raw_path = f"{raw_data_path}/sf={args.sf}"
    warehouse_dir = f"{raw_data_path}/warehouse"
    batch_id = args.batch_id

    spark = (
        __import__("pyspark.sql", fromlist=["SparkSession"])
        .SparkSession.builder.appName("TPC-DI-v3-Dataproc")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
        .getOrCreate()
    )

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database} LOCATION '{warehouse_dir}/{database}.db'")
    spark.sql(f"USE {database}")

    # gold_dim_messages (for DQ if run later)
    spark.sql(f"DROP TABLE IF EXISTS {database}.gold_dim_messages")
    spark.sql(f"""
        CREATE TABLE {database}.gold_dim_messages (
            message_timestamp TIMESTAMP NOT NULL,
            batch_id INT NOT NULL,
            originating_table STRING NOT NULL,
            message_text STRING NOT NULL,
            message_type STRING NOT NULL,
            component_name STRING,
            severity STRING
        ) USING DELTA
    """)

    import tpcdi_metrics as metrics
    _table_details = []
    _steps = []
    job_start = time.time()

    # ---- Bronze: v2 customer_mgmt/finwire if present, then PySpark batch
    t0 = time.time()
    base_dir = str(V2_DATAPROC)
    for name, table_short in [("load_bronze_customer_mgmt", "bronze_customer_mgmt"), ("load_bronze_finwire", "bronze_finwire")]:
        script = Path(base_dir) / "sql" / "bronze" / "batch" / f"{name}.py"
        if script.exists():
            with open(script) as f:
                code = f.read()
            g = {"spark": spark, "database": database, "batch_id": batch_id, "full_raw_path": full_raw_path}
            exec(compile(code, str(script), "exec"), g)
    load_bronze_batch(spark, database, batch_id, full_raw_path, warehouse_dir, BRONZE_ORDER)
    for t in ["bronze_customer_mgmt", "bronze_finwire"] + BRONZE_ORDER:
        if spark.catalog.tableExists(f"{database}.{t}"):
            rc, sz, _ = metrics.get_table_stats(spark, database, t, use_refresh=False)
            metrics.record_table_load(_table_details, t, 0, rc, sz, database)
    _steps.append({"step_name": "bronze_etl", "duration_seconds": time.time() - t0, "rows_processed": sum(d.get("row_count", 0) for d in _table_details)})

    # ---- Silver: v2 customers/accounts if present, then PySpark batch
    t0 = time.time()
    n_before = len(_table_details)
    for name, table_short in [("transform_silver_customers", "silver_customers"), ("transform_silver_accounts", "silver_accounts")]:
        script = Path(base_dir) / "sql" / "silver" / "batch" / f"{name}.py"
        if script.exists():
            with open(script) as f:
                code = f.read()
            g = {"spark": spark, "database": database, "batch_id": batch_id}
            exec(compile(code, str(script), "exec"), g)
            rc, sz, _ = metrics.get_table_stats(spark, database, table_short, use_refresh=False)
            metrics.record_table_load(_table_details, table_short, 0, rc, sz, database)
    transform_silver_batch(spark, database, batch_id, SILVER_ORDER)
    for t in ["silver_customers", "silver_accounts"] + SILVER_ORDER:
        if spark.catalog.tableExists(f"{database}.{t}") and not any(d.get("table", "").endswith(f".{t}") for d in _table_details):
            rc, sz, _ = metrics.get_table_stats(spark, database, t, use_refresh=False)
            metrics.record_table_load(_table_details, t, 0, rc, sz, database)
    _steps.append({"step_name": "silver_etl", "duration_seconds": time.time() - t0, "rows_processed": sum(d.get("row_count", 0) for d in _table_details[n_before:])})

    # ---- Gold
    t0 = time.time()
    n_gold_before = len(_table_details)
    load_gold_batch(spark, database, batch_id, GOLD_TABLE_ORDER)
    for t in GOLD_TABLE_ORDER:
        if spark.catalog.tableExists(f"{database}.{t}"):
            rc, sz, _ = metrics.get_table_stats(spark, database, t, use_refresh=False)
            metrics.record_table_load(_table_details, t, 0, rc, sz, database)
    GOLD_LOAD_NAMES = {
        "gold_dim_date", "gold_dim_time", "gold_dim_status_type", "gold_dim_trade_type", "gold_dim_industry",
        "gold_dim_customer", "gold_dim_account", "gold_dim_broker", "gold_dim_company", "gold_dim_security",
        "gold_fact_trade", "gold_fact_cash_balances", "gold_fact_holdings", "gold_fact_market_history", "gold_fact_watches",
        "gold_financials", "gold_prospect",
    }
    _gold_rows = sum(d.get("row_count", 0) for d in _table_details[n_gold_before:] if d.get("table", "").split(".")[-1] in GOLD_LOAD_NAMES)
    _steps.append({"step_name": "gold_etl", "duration_seconds": time.time() - t0, "rows_processed": _gold_rows})

    metrics.print_benchmark_report(spark, _steps, _table_details, job_start, time.time(), database, "batch", str(args.sf), 0.0)
    if getattr(args, "metrics_output", ""):
        metrics.save_metrics_output(spark, _steps, _table_details, job_start, time.time(), database, "batch", str(args.sf), args.metrics_output, batch_id=batch_id)


if __name__ == "__main__":
    main()
