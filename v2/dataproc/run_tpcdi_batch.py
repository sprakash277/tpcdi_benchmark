#!/usr/bin/env python3
"""
TPC-DI v2 Dataproc runner: same pattern as v2/databricks/run_tpcdi_batch.
Runs Bronze → Silver → Gold using Delta tables; SQL under sql/; Python for customer_mgmt/finwire and silver customers/accounts.
Use with: spark-submit run_tpcdi_batch.py --database tpcdi_dw --raw-data-path gs://bucket/tpcdi --load-type batch --sf 10
Requires: Delta Lake JAR, spark-xml JAR (for CustomerMgmt).
"""

import argparse
import os
import re
import sys
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional

# Bronze batch: table short name -> (path_suffix under raw_path/sf=X/, source_file label)
BRONZE_BATCH_PATHS = {
    "bronze_date": ("Batch1/Date.txt", "Date.txt"),
    "bronze_time": ("Batch1/Time.txt", "Time.txt"),
    "bronze_status_type": ("Batch1/StatusType.txt", "StatusType.txt"),
    "bronze_trade_type": ("Batch1/TradeType.txt", "TradeType.txt"),
    "bronze_industry": ("Batch1/Industry.txt", "Industry.txt"),
    "bronze_tax_rate": ("Batch1/TaxRate.txt", "TaxRate.txt"),
    "bronze_trade": ("Batch1/Trade.txt", "Trade.txt"),
    "bronze_daily_market": ("Batch1/DailyMarket.txt", "DailyMarket.txt"),
    "bronze_cash_transaction": ("Batch1/CashTransaction.txt", "CashTransaction.txt"),
    "bronze_holding_history": ("Batch1/HoldingHistory.txt", "HoldingHistory.txt"),
    "bronze_watch_history": ("Batch1/WatchHistory.txt", "WatchHistory.txt"),
    "bronze_hr": ("Batch1/HR.csv", "HR.csv"),
    "bronze_prospect": ("Batch1/Prospect.csv", "Prospect.csv"),
}
# Bronze incremental: table -> (path pattern with {batch_id}, source_file)
BRONZE_INCR_PATHS = {
    "bronze_customer": ("Batch{batch_id}/Customer.txt", "Customer.txt"),
    "bronze_account": ("Batch{batch_id}/Account.txt", "Account.txt"),
    "bronze_trade": ("Batch{batch_id}/Trade.txt", "Trade.txt"),
    "bronze_daily_market": ("Batch{batch_id}/DailyMarket.txt", "DailyMarket.txt"),
    "bronze_cash_transaction": ("Batch{batch_id}/CashTransaction.txt", "CashTransaction.txt"),
    "bronze_holding_history": ("Batch{batch_id}/HoldingHistory.txt", "HoldingHistory.txt"),
    "bronze_watch_history": ("Batch{batch_id}/WatchHistory.txt", "WatchHistory.txt"),
    "bronze_prospect": ("Batch{batch_id}/Prospect.csv", "Prospect.csv"),
}


def _get_gcp_machine_type() -> Optional[str]:
    """Get current VM machine type from GCP metadata (e.g. on Dataproc). Returns short name like n2d-standard-16. v1-style."""
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/machine-type",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            path = resp.read().decode().strip()
            return path.split("/")[-1] if path else None
    except Exception:
        return None


def _get_dataproc_worker_count_from_metadata() -> Optional[int]:
    """Get number of worker nodes from Dataproc GCP instance metadata (attributes/dataproc-worker-count). v1-style."""
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/attributes/dataproc-worker-count",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            val = resp.read().decode().strip()
            return int(val) if val else None
    except Exception:
        return None


def _get_executor_count(spark) -> Optional[int]:
    """Get number of executors (worker nodes) from Spark. Excludes driver. v1-style."""
    try:
        sc = spark.sparkContext
        status = sc._jsc.sc().getExecutorMemoryStatus()
        count = status.size() - 1
        return max(0, count) if count is not None else None
    except Exception:
        return None


def _get_cluster_info(spark, cluster_instance_type: Optional[str], cluster_worker_count: Optional[int], cluster_master_type: Optional[str]):
    """Return (worker_type, worker_count, driver_type) from args or GCP/Spark metadata. v1-style."""
    driver_type = cluster_master_type or _get_gcp_machine_type()
    worker_type = cluster_instance_type or driver_type
    worker_count = cluster_worker_count
    if worker_count is None:
        worker_count = _get_executor_count(spark)
        if worker_count is None or worker_count == 0:
            worker_count = _get_dataproc_worker_count_from_metadata()
    return (worker_type, worker_count, driver_type)


def adapt_sql(content: str, database: str, batch_id: int, raw_path: str, use_pipe: bool = False) -> str:
    """Replace placeholders and optionally split_part -> element_at(split) for Spark SQL."""
    content = content.replace("__CATALOG__.__SCHEMA__", "__DATABASE__")
    content = content.replace("__DATABASE__", database)
    content = content.replace("__BATCH_ID__", str(batch_id))
    content = content.replace("__RAW_DATA_PATH__", raw_path)
    if use_pipe:
        # split_part(col, '|', n) -> element_at(split(col, '\\|'), n) for Spark
        content = re.sub(
            r"split_part\s*\(\s*([^,]+)\s*,\s*['\"]?\s*\|\s*['\"]?\s*,\s*(\d+)\s*\)",
            r"element_at(split(\1, '\\|'), \2)",
            content,
            flags=re.IGNORECASE,
        )
    return content


def main():
    parser = argparse.ArgumentParser(description="TPC-DI v2 Dataproc: run_tpcdi_batch (Delta)")
    parser.add_argument("--database", default="tpcdi_dw", help="Hive database base name (sf appended as _sf{N}, e.g. tpcdi_dw_sf10)")
    parser.add_argument("--raw-data-path", required=True, help="Base path to TPC-DI data (e.g. gs://bucket/tpcdi)")
    parser.add_argument("--sf", type=int, default=10, help="Scale factor")
    parser.add_argument("--batch-id", type=int, default=1, help="Batch ID (1=batch, 2+=incremental)")
    parser.add_argument("--load-type", choices=["batch", "incremental"], default="batch")
    parser.add_argument("--sql-base-path", default="", help="Base dir for sql/ (default: script dir)")
    parser.add_argument("--xml-format", default="com.databricks.spark.xml", help="XML reader for CustomerMgmt")
    parser.add_argument("--service-account-email", default="", help="Service account email for GCS (optional)")
    parser.add_argument("--service-account-key-file", default="", help="Path to SA JSON key file (local or gs://); local required for Spark GCS auth")
    parser.add_argument("--metrics-output", default="gs://sumit_prakash_gcs/tpcdi/metrics", help="Path to save metrics JSON (GCS or local; default gs://sumit_prakash_gcs/tpcdi/metrics)")
    parser.add_argument("--cluster-instance-type", default="", help="Worker node type for metrics (e.g. n2d-standard-16); auto-detected from GCP metadata if not set")
    parser.add_argument("--cluster-worker-count", type=int, default=None, help="Number of worker nodes for metrics; auto-detected from Spark/metadata if not set")
    parser.add_argument("--cluster-master-type", default="", help="Driver node type for metrics; auto-detected from GCP metadata if not set")
    args = parser.parse_args()

    database = args.database
    # Include sf in database name so different scale factors use different DBs (e.g. tpcdi_dw_sf10)
    if not database.endswith(f"_sf{args.sf}"):
        database = f"{database}_sf{args.sf}"
    raw_data_path = args.raw_data_path.rstrip("/")
    full_raw_path = f"{raw_data_path}/sf={args.sf}"
    batch_id = args.batch_id
    load_type = args.load_type

    # Use same GCS bucket as raw data for Spark internal catalog (warehouse); avoids creating DB under /tmp
    warehouse_dir = f"{raw_data_path}/warehouse"
    database_location = f"{warehouse_dir}/{database}.db"

    script_dir = Path(__file__).resolve().parent
    base_dir = args.sql_base_path or str(script_dir)
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)
    import tpcdi_metrics as metrics

    from pyspark.sql import SparkSession
    from pyspark import SparkFiles
    spark = (
        SparkSession.builder.appName("TPC-DI-v2-Dataproc")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.delta.logStore.gs.impl", "io.delta.storage.GCSLogStore")
        .config("spark.sql.defaultSerializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )
    # On Dataproc only the main script and --py-files are uploaded; sql/ is missing. Unzip sql.zip if found.
    if not (Path(base_dir) / "sql").exists():
        zip_path = None
        # Try SparkFiles (--files sql.zip), then root dir, then script dir, then cwd
        try:
            zip_path = SparkFiles.get("sql.zip")
        except Exception:
            pass
        if not zip_path or not Path(zip_path).exists():
            try:
                root = SparkFiles.getRootDirectory()
                if root:
                    for name in ("sql.zip",):
                        p = Path(root) / name
                        if p.exists():
                            zip_path = str(p)
                            break
                    if not zip_path and Path(root).exists():
                        for f in Path(root).iterdir():
                            if f.name == "sql.zip" or f.name.endswith("sql.zip"):
                                zip_path = str(f)
                                break
            except Exception:
                pass
        if not zip_path or not Path(zip_path).exists():
            for d in (script_dir, Path(os.getcwd())):
                p = d / "sql.zip" if isinstance(d, Path) else Path(d) / "sql.zip"
                if p.exists():
                    zip_path = str(p)
                    break
        if zip_path and Path(zip_path).exists():
            with zipfile.ZipFile(zip_path, "r") as z:
                z.extractall(script_dir)
            base_dir = str(script_dir)
    sql_from_gcs = base_dir.startswith("gs://")
    if not sql_from_gcs and not (Path(base_dir) / "sql").exists():
        raise FileNotFoundError(
            f"sql/ not found under {base_dir}. When submitting to Dataproc: (1) use run_dataproc_job.sh from v2/dataproc (it creates sql.zip and passes --files=sql.zip), "
            "or (2) upload sql/ to GCS and pass --sql-base-path gs://bucket/path/to/v2/dataproc so the script reads SQL from GCS."
        )
    # Configure GCS service account auth if provided. Key file path must exist on all nodes (driver + executors);
    # a path that only exists on the driver (e.g. temp file from gs:// download) causes NPE on executors.
    service_account_email = (args.service_account_email or "").strip()
    service_account_key_file = (args.service_account_key_file or "").strip()
    key_file_is_driver_only = False
    if service_account_email or service_account_key_file:
        key_file = service_account_key_file
        if key_file.startswith("gs://"):
            # Download to temp on driver for any driver-side reads. Do NOT set keyfile in Hadoop config:
            # executors do not have that temp path and GCS connector would NPE when opening it.
            import tempfile
            try:
                content = spark.sparkContext.wholeTextFiles(key_file).collect()
                if content:
                    _, json_str = content[0]
                    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
                        f.write(json_str)
                        key_file = f.name
                    key_file_is_driver_only = True
            except Exception as e:
                print(f"WARN: Could not download key from {service_account_key_file}: {e}; using default GCS credentials")
                key_file = ""
        use_keyfile = service_account_email and key_file and os.path.isfile(key_file) and not key_file_is_driver_only
        try:
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            if use_keyfile:
                hadoop_conf.set("fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE")
                hadoop_conf.set("fs.gs.auth.service.account.email", service_account_email)
                hadoop_conf.set("fs.gs.auth.service.account.keyfile", key_file)
                print(f"Configured GCS access with service account key file: {service_account_email}")
            elif service_account_email:
                hadoop_conf.set("fs.gs.auth.service.account.email", service_account_email)
                if key_file_is_driver_only:
                    print(f"GCS: using service account email {service_account_email}; executors use default credentials (cluster SA must have bucket access)")
                else:
                    print(f"Configured GCS access with service account email: {service_account_email}")
        except Exception as e:
            print(f"WARN: Could not set GCS service account config: {e}")

    # Create database with explicit GCS location so tables live in same bucket as raw data
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database} LOCATION '{database_location}'")
    spark.sql(f"USE {database}")

    def read_sql_file(rel_path: str) -> str:
        if base_dir and base_dir.startswith("gs://"):
            path = base_dir.rstrip("/") + "/" + rel_path.replace("\\", "/")
        else:
            path = os.path.join(base_dir, rel_path) if base_dir else rel_path
        if path.startswith("gs://"):
            # Read SQL from GCS (e.g. when --sql-base-path gs://bucket/v2/dataproc)
            try:
                parts = spark.sparkContext.wholeTextFiles(path).collect()
                if parts:
                    return parts[0][1]
            except Exception as e:
                raise FileNotFoundError(f"Could not read {path}: {e}") from e
            raise FileNotFoundError(f"Empty or missing: {path}")
        with open(path, "r") as f:
            return f.read()

    def run_sql(content: str, use_pipe: bool = False):
        content = adapt_sql(content, database, batch_id, full_raw_path, use_pipe=use_pipe)
        for stmt in content.split(";"):
            stmt = stmt.strip()
            if stmt and not all(
                line.strip().startswith("--") or not line.strip() for line in stmt.split("\n")
            ):
                spark.sql(stmt)

    _table_details = []
    _steps = []
    _total_refresh_seconds = 0.0
    job_start_time = time.time()

    # Cluster config for metrics (v1-style: from args or GCP metadata / Spark)
    _cluster_instance_type = (getattr(args, "cluster_instance_type", "") or "").strip() or None
    _cluster_master_type = (getattr(args, "cluster_master_type", "") or "").strip() or None
    _cluster_worker_count = getattr(args, "cluster_worker_count", None)
    _cluster_instance_type, _cluster_worker_count, _cluster_master_type = _get_cluster_info(
        spark, _cluster_instance_type, _cluster_worker_count, _cluster_master_type
    )

    def run_sql_timed(rel_path: str, use_pipe: bool = False):
        nonlocal _total_refresh_seconds
        table_name = metrics.sql_file_to_table_name(rel_path)
        t0 = time.time()
        run_sql(read_sql_file(rel_path), use_pipe=use_pipe)
        duration = time.time() - t0
        if table_name:
            rc, sz, refresh_sec = metrics.get_table_stats(spark, database, table_name, use_refresh=True)
            _total_refresh_seconds += refresh_sec
            metrics.record_table_load(_table_details, table_name, duration + refresh_sec, rc, sz, database)

    def run_sql_multi_timed(rel_path: str, use_pipe: bool = False):
        nonlocal _total_refresh_seconds
        table_name = metrics.sql_file_to_table_name(rel_path)
        t0 = time.time()
        run_sql(read_sql_file(rel_path), use_pipe=use_pipe)
        duration = time.time() - t0
        if table_name:
            rc, sz, refresh_sec = metrics.get_table_stats(spark, database, table_name, use_refresh=False)
            _total_refresh_seconds += refresh_sec
            metrics.record_table_load(_table_details, table_name, duration + refresh_sec, rc, sz, database)

    # ---- Bronze batch: create temp view from path, then run SQL that uses FROM _tmp_*
    def create_bronze_temp_view(table_short: str, path_suffix: str):
        path = f"{full_raw_path}/{path_suffix}"
        df = spark.read.format("text").load(path)
        tmp_view = f"_tmp_{table_short}"
        df.createOrReplaceTempView(tmp_view)

    def drop_bronze_table_and_path(db: str, table: str):
        """Drop table then remove its warehouse path if present (v1-style). Avoids LOCATION_ALREADY_EXISTS on re-runs."""
        spark.sql(f"DROP TABLE IF EXISTS {db}.{table}")
        table_path = f"{warehouse_dir}/{db}.db/{table}"
        try:
            jvm = spark._jvm
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            path = jvm.org.apache.hadoop.fs.Path(table_path)
            if fs.exists(path):
                fs.delete(path, True)
        except Exception as e:
            print(f"WARN: Could not delete path {table_path}: {e}")

    def run_bronze_load_sql(rel_path: str):
        """Run bronze load SQL; expects content to use FROM _tmp_<table> (Dataproc sql/bronze/)."""
        table_name = metrics.sql_file_to_table_name(rel_path)
        if not table_name or table_name not in BRONZE_BATCH_PATHS:
            run_sql(adapt_sql(read_sql_file(rel_path), database, batch_id, full_raw_path), use_pipe=False)
            return
        path_suffix, _ = BRONZE_BATCH_PATHS[table_name]
        create_bronze_temp_view(table_name, path_suffix)
        # v1-style: drop table first (and remove path) so CREATE TABLE AS SELECT does not hit LOCATION_ALREADY_EXISTS
        drop_bronze_table_and_path(database, table_name)
        t0 = time.time()
        content = read_sql_file(rel_path)
        content = content.replace("__DATABASE__", database).replace("__CATALOG__.__SCHEMA__", database).replace("__BATCH_ID__", str(batch_id))
        run_sql(content)
        rc, sz, refresh_sec = metrics.get_table_stats(spark, database, table_name, use_refresh=True)
        metrics.record_table_load(_table_details, table_name, time.time() - t0 + refresh_sec, rc, sz, database)

    # ---- Incremental bronze: create temp view then run Dataproc SQL (FROM _tmp_*)
    def run_bronze_incremental_sql(rel_path: str):
        table_name = metrics.sql_file_to_table_name(rel_path)
        if not table_name or table_name not in BRONZE_INCR_PATHS:
            run_sql_multi_timed(rel_path)
            return
        path_pattern, _ = BRONZE_INCR_PATHS[table_name]
        path_suffix = path_pattern.format(batch_id=batch_id)
        create_bronze_temp_view(table_name, path_suffix)
        run_sql_multi_timed(rel_path)

    # ========== INCREMENTAL ==========
    if load_type == "incremental":
        bronze_incr = [
            "sql/bronze/incremental/load_bronze_incremental_customer.sql",
            "sql/bronze/incremental/load_bronze_incremental_account.sql",
            "sql/bronze/incremental/load_bronze_incremental_trade.sql",
            "sql/bronze/incremental/load_bronze_incremental_daily_market.sql",
            "sql/bronze/incremental/load_bronze_incremental_cash_transaction.sql",
            "sql/bronze/incremental/load_bronze_incremental_holding_history.sql",
            "sql/bronze/incremental/load_bronze_incremental_watch_history.sql",
            "sql/bronze/incremental/load_bronze_incremental_prospect.sql",
        ]
        silver_incr = [
            "sql/silver/incremental/transform_silver_incremental_customers.sql",
            "sql/silver/incremental/transform_silver_incremental_accounts.sql",
            "sql/silver/incremental/transform_silver_incremental_trades.sql",
            "sql/silver/incremental/transform_silver_incremental_daily_market.sql",
            "sql/silver/incremental/transform_silver_incremental_cash_transaction.sql",
            "sql/silver/incremental/transform_silver_incremental_holding_history.sql",
            "sql/silver/incremental/transform_silver_incremental_watch_history.sql",
            "sql/silver/incremental/transform_silver_incremental_prospect.sql",
        ]
        gold_incr = [
            "sql/gold/incremental/load_gold_incremental_dim_customer.sql",
            "sql/gold/incremental/load_gold_incremental_dim_account.sql",
            "sql/gold/incremental/load_gold_incremental_dim_security.sql",
            "sql/gold/incremental/load_gold_incremental_dim_company.sql",
            "sql/gold/incremental/load_gold_incremental_financials.sql",
            "sql/gold/incremental/load_gold_incremental_fact_trade.sql",
            "sql/gold/incremental/load_gold_incremental_dim_messages.sql",
            "sql/gold/incremental/load_gold_incremental_fact_market_history.sql",
            "sql/gold/incremental/load_gold_incremental_fact_cash_balances.sql",
            "sql/gold/incremental/load_gold_incremental_fact_holdings.sql",
            "sql/gold/incremental/load_gold_incremental_fact_watches.sql",
            "sql/gold/incremental/load_gold_incremental_prospect.sql",
        ]
        gold_optimize = [
            "sql/gold/optimize/optimize_gold_dim_company.sql",
            "sql/gold/optimize/optimize_gold_dim_customer.sql",
            "sql/gold/optimize/optimize_gold_dim_account.sql",
            "sql/gold/optimize/optimize_gold_dim_security.sql",
            "sql/gold/optimize/optimize_gold_dim_broker.sql",
            "sql/gold/optimize/optimize_gold_prospect.sql",
            "sql/gold/optimize/optimize_gold_fact_trade.sql",
            "sql/gold/optimize/optimize_gold_fact_holdings.sql",
            "sql/gold/optimize/optimize_gold_financials.sql",
        ]
        # Ensure bronze_customer and bronze_account exist (incremental-only tables)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {database}.bronze_customer (
                raw_line STRING, _batch_id BIGINT, _load_timestamp TIMESTAMP, _source_file STRING
            ) USING delta
        """)
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {database}.bronze_account (
                raw_line STRING, _batch_id BIGINT, _load_timestamp TIMESTAMP, _source_file STRING
            ) USING delta
        """)
        _n = len(_table_details)
        t0 = time.time()
        for f in bronze_incr:
            print(f"Bronze: {f}")
            run_bronze_incremental_sql(f)
        _steps.append({"step_name": "bronze_etl", "duration_seconds": time.time() - t0, "rows_processed": sum(d["row_count"] for d in _table_details[_n:])})
        _n = len(_table_details)
        t0 = time.time()
        for f in silver_incr:
            print(f"Silver: {f}")
            run_sql_multi_timed(f, use_pipe=True)
        _steps.append({"step_name": "silver_etl", "duration_seconds": time.time() - t0, "rows_processed": sum(d["row_count"] for d in _table_details[_n:])})
        t0 = time.time()
        for f in gold_optimize:
            run_sql(read_sql_file(f).replace("__CATALOG__.__SCHEMA__", database))
        run_sql(read_sql_file("sql/gold/create_gold_dim_messages.sql").replace("__CATALOG__.__SCHEMA__", database))
        dq_files_incr = [
            "sql/dq/dq_silver_date.sql", "sql/dq/dq_silver_status_type.sql", "sql/dq/dq_silver_trade_type.sql", "sql/dq/dq_silver_industry.sql",
            "sql/dq/dq_silver_companies.sql", "sql/dq/dq_silver_securities.sql", "sql/dq/dq_silver_financials.sql",
            "sql/dq/dq_silver_customers.sql", "sql/dq/dq_silver_accounts.sql", "sql/dq/dq_silver_trades.sql", "sql/dq/dq_silver_daily_market.sql",
            "sql/dq/dq_silver_cash_transaction.sql", "sql/dq/dq_silver_holding_history.sql", "sql/dq/dq_silver_watch_history.sql", "sql/dq/dq_silver_prospect.sql",
        ]
        for rel in dq_files_incr:
            try:
                run_sql(read_sql_file(rel))
            except Exception as e:
                print(f"DQ {rel} warning: {e}")
        _n = len(_table_details)
        t0 = time.time()
        for f in gold_incr:
            print(f"Gold: {f}")
            run_sql_multi_timed(f)
        _steps.append({"step_name": "gold_etl", "duration_seconds": time.time() - t0, "rows_processed": sum(d["row_count"] for d in _table_details[_n:])})
        metrics.print_benchmark_report(spark, _steps, _table_details, job_start_time, time.time(), database, load_type, str(args.sf), _total_refresh_seconds, cluster_worker_count=_cluster_worker_count, cluster_instance_type=_cluster_instance_type, cluster_master_type=_cluster_master_type)
        if getattr(args, "metrics_output", ""):
            metrics.save_metrics_output(spark, _steps, _table_details, job_start_time, time.time(), database, load_type, str(args.sf), args.metrics_output, batch_id=batch_id, total_refresh_seconds=_total_refresh_seconds, service_account_key_file=getattr(args, "service_account_key_file", None), cluster_worker_count=_cluster_worker_count, cluster_instance_type=_cluster_instance_type, cluster_master_type=_cluster_master_type)
        return

    # ========== BATCH ==========
    bronze_before = [
        "sql/bronze/load_bronze_date.sql",
        "sql/bronze/load_bronze_time.sql",
        "sql/bronze/load_bronze_status_type.sql",
        "sql/bronze/load_bronze_trade_type.sql",
        "sql/bronze/load_bronze_industry.sql",
        "sql/bronze/load_bronze_tax_rate.sql",
    ]
    bronze_after = [
        "sql/bronze/load_bronze_trade.sql",
        "sql/bronze/load_bronze_daily_market.sql",
        "sql/bronze/load_bronze_cash_transaction.sql",
        "sql/bronze/load_bronze_holding_history.sql",
        "sql/bronze/load_bronze_watch_history.sql",
        "sql/bronze/load_bronze_hr.sql",
        "sql/bronze/load_bronze_prospect.sql",
    ]

    _bronze_start = time.time()
    for rel in bronze_before:
        print(f"Bronze SQL: {rel}")
        run_bronze_load_sql(rel)
    # Bronze Python: customer_mgmt, finwire (run via exec or import - we'll run inline load here)
    for name, table_short in [("load_bronze_customer_mgmt", "bronze_customer_mgmt"), ("load_bronze_finwire", "bronze_finwire")]:
        print(f"Bronze Python: {name}")
        t0 = time.time()
        try:
            load_bronze_batch = Path(base_dir) / "sql" / "bronze" / "batch" / f"{name}.py"
            if load_bronze_batch.exists():
                with open(load_bronze_batch) as f:
                    code = f.read()
                code = code.replace("dbutils.widgets.get(", "# dbutils\n    _get = lambda x: ")
                # Provide globals: spark, database, full_raw_path, batch_id
                g = {"spark": spark, "database": database, "full_raw_path": full_raw_path, "batch_id": batch_id, "xml_format": getattr(args, "xml_format", "com.databricks.spark.xml"), "args": args}
                exec(compile(code, str(load_bronze_batch), "exec"), g)
            else:
                # Minimal inline: finwire
                if "finwire" in name:
                    batch1 = f"{full_raw_path}/Batch1"
                    from pyspark.sql.functions import lit, current_timestamp, col, length
                    files = [p.path for p in spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration()).listStatus(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(batch1)) if "FINWIRE" in p.getPath().getName() and not p.getPath().getName().endswith(".csv")]
                    if not files:
                        raise FileNotFoundError(f"No FINWIRE files under {batch1}")
                    df = spark.read.format("text").load(files).withColumnRenamed("value", "raw_line").withColumn("_batch_id", lit(batch_id)).withColumn("_load_timestamp", current_timestamp()).withColumn("_source_file", lit("FINWIRE*")).filter(col("raw_line").isNotNull()).filter(length(col("raw_line")) >= 18)
                    df.write.format("delta").mode("overwrite").saveAsTable(f"{database}.bronze_finwire")
                else:
                    raise FileNotFoundError(f"Bronze batch script not found: {load_bronze_batch}")
        except Exception as e:
            print(f"Bronze {name} failed: {e}")
            raise
        rc, sz, _ = metrics.get_table_stats(spark, database, table_short, use_refresh=True)
        metrics.record_table_load(_table_details, table_short, time.time() - t0, rc, sz, database)
    for rel in bronze_after:
        print(f"Bronze SQL: {rel}")
        run_bronze_load_sql(rel)
    _steps.append({"step_name": "bronze_etl", "duration_seconds": time.time() - _bronze_start, "rows_processed": sum(d["row_count"] for d in _table_details)})

    _silver_start = time.time()
    _n_silver = len(_table_details)
    silver_sql = [
        "sql/silver/transform_silver_date.sql", "sql/silver/transform_silver_time.sql",
        "sql/silver/transform_silver_status_type.sql", "sql/silver/transform_silver_trade_type.sql",
        "sql/silver/transform_silver_industry.sql", "sql/silver/transform_silver_tax_rate.sql",
        "sql/silver/transform_silver_companies.sql", "sql/silver/transform_silver_securities.sql",
        "sql/silver/transform_silver_financials.sql", "sql/silver/transform_silver_trades.sql",
        "sql/silver/transform_silver_daily_market.sql", "sql/silver/transform_silver_cash_transaction.sql",
        "sql/silver/transform_silver_holding_history.sql", "sql/silver/transform_silver_watch_history.sql",
        "sql/silver/transform_silver_prospect.sql",
    ]
    for rel in silver_sql:
        print(f"Silver SQL: {rel}")
        run_sql_timed(rel, use_pipe=True)
    # Silver Python: customers, accounts (optional - if scripts exist)
    for name, table_short in [("transform_silver_customers", "silver_customers"), ("transform_silver_accounts", "silver_accounts")]:
        script = Path(base_dir) / "sql" / "silver" / "batch" / f"{name}.py"
        if script.exists():
            print(f"Silver Python: {name}")
            t0 = time.time()
            with open(script) as f:
                code = f.read()
            g = {"spark": spark, "database": database, "batch_id": batch_id}
            exec(compile(code, str(script), "exec"), g)
            rc, sz, _ = metrics.get_table_stats(spark, database, table_short, use_refresh=True)
            metrics.record_table_load(_table_details, table_short, time.time() - t0, rc, sz, database)
    _steps.append({"step_name": "silver_etl", "duration_seconds": time.time() - _silver_start, "rows_processed": sum(d["row_count"] for d in _table_details[_n_silver:])})

    run_sql(read_sql_file("sql/gold/create_gold_dim_messages.sql").replace("__CATALOG__.__SCHEMA__", database))
    dq_files = [
        "sql/dq/dq_silver_date.sql", "sql/dq/dq_silver_status_type.sql", "sql/dq/dq_silver_trade_type.sql", "sql/dq/dq_silver_industry.sql",
        "sql/dq/dq_silver_companies.sql", "sql/dq/dq_silver_securities.sql", "sql/dq/dq_silver_financials.sql",
        "sql/dq/dq_silver_customers.sql", "sql/dq/dq_silver_accounts.sql", "sql/dq/dq_silver_trades.sql", "sql/dq/dq_silver_daily_market.sql",
        "sql/dq/dq_silver_cash_transaction.sql", "sql/dq/dq_silver_holding_history.sql", "sql/dq/dq_silver_watch_history.sql", "sql/dq/dq_silver_prospect.sql",
    ]
    _dq_start = time.time()
    for rel in dq_files:
        try:
            run_sql(read_sql_file(rel))
        except Exception as e:
            print(f"DQ {rel} warning: {e}")
    _steps.append({"step_name": "silver_dq", "duration_seconds": time.time() - _dq_start, "rows_processed": 0})

    # gold_etl rows = sum of these 17 tables only (same definition as Databricks for comparable metrics)
    GOLD_LOAD_TABLE_NAMES = {
        "gold_dim_date", "gold_dim_time", "gold_dim_status_type", "gold_dim_trade_type", "gold_dim_industry",
        "gold_dim_customer", "gold_dim_account", "gold_dim_broker", "gold_dim_company", "gold_dim_security",
        "gold_fact_trade", "gold_fact_cash_balances", "gold_fact_holdings", "gold_fact_market_history", "gold_fact_watches",
        "gold_financials", "gold_prospect",
    }
    _gold_start = time.time()
    _n_gold = len(_table_details)
    gold_sql = [
        "sql/gold/load_gold_dim_date.sql", "sql/gold/load_gold_dim_time.sql", "sql/gold/load_gold_dim_status_type.sql",
        "sql/gold/load_gold_dim_trade_type.sql", "sql/gold/load_gold_dim_industry.sql", "sql/gold/load_gold_dim_customer.sql",
        "sql/gold/load_gold_dim_account.sql", "sql/gold/load_gold_dim_broker.sql", "sql/gold/load_gold_dim_company.sql",
        "sql/gold/load_gold_dim_security.sql", "sql/gold/load_gold_fact_trade.sql", "sql/gold/load_gold_fact_cash_balances.sql",
        "sql/gold/load_gold_fact_holdings.sql", "sql/gold/load_gold_fact_market_history.sql", "sql/gold/load_gold_fact_watches.sql",
        "sql/gold/load_gold_financials.sql", "sql/gold/load_gold_prospect.sql",
    ]
    for rel in gold_sql:
        print(f"Gold SQL: {rel}")
        run_sql_timed(rel)
    _gold_rows = sum(d["row_count"] for d in _table_details[_n_gold:] if d.get("table", "").split(".")[-1] in GOLD_LOAD_TABLE_NAMES)
    _steps.append({"step_name": "gold_etl", "duration_seconds": time.time() - _gold_start, "rows_processed": _gold_rows})

    metrics.print_benchmark_report(spark, _steps, _table_details, job_start_time, time.time(), database, load_type, str(args.sf), _total_refresh_seconds, cluster_worker_count=_cluster_worker_count, cluster_instance_type=_cluster_instance_type, cluster_master_type=_cluster_master_type)
    if getattr(args, "metrics_output", ""):
        metrics.save_metrics_output(spark, _steps, _table_details, job_start_time, time.time(), database, load_type, str(args.sf), args.metrics_output, batch_id=batch_id, total_refresh_seconds=_total_refresh_seconds, service_account_key_file=getattr(args, "service_account_key_file", None), cluster_worker_count=_cluster_worker_count, cluster_instance_type=_cluster_instance_type, cluster_master_type=_cluster_master_type)


if __name__ == "__main__":
    main()
