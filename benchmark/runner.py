"""
Main benchmark runner for TPC-DI benchmark.
Orchestrates ETL execution on Databricks or Dataproc platforms.
"""

import logging
import re
import urllib.request
from typing import Optional, Tuple
from pyspark.sql import SparkSession

from benchmark.config import BenchmarkConfig, Platform, LoadType
from benchmark.metrics import MetricsCollector
from benchmark.etl.table_timing import (
    clear as clear_table_timing,
    configure as table_timing_configure,
    set_job_start as table_timing_job_start,
    set_job_end as table_timing_job_end,
    log_final_summary as table_timing_log_final,
)
from benchmark.platforms.databricks import DatabricksPlatform
from benchmark.platforms.dataproc import DataprocPlatform

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Suppress py4j (PySpark JVM bridge) DEBUG/INFO logs
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("py4j.clientserver").setLevel(logging.WARNING)


def create_spark_session(config: BenchmarkConfig) -> SparkSession:
    """
    Create and configure SparkSession based on platform.
    
    Args:
        config: Benchmark configuration
    
    Returns:
        Configured SparkSession
    """
    if config.platform == Platform.DATABRICKS:
        # Databricks: SparkSession should already be available
        try:
            spark = SparkSession.builder.getOrCreate()
            logger.info("Using existing Databricks SparkSession")
            return spark
        except Exception:
            # Fallback: create new session
            logger.warning("Could not get existing SparkSession, creating new one")
            return SparkSession.builder.appName("TPC-DI-Benchmark").getOrCreate()
    
    elif config.platform == Platform.DATAPROC:
        # Dataproc: create SparkSession with GCS support
        builder = SparkSession.builder.appName("TPC-DI-Benchmark-Dataproc")
        
        if config.spark_master:
            builder = builder.master(config.spark_master)
        
        # Use GCS for Spark warehouse so CREATE DATABASE / tables use gs://, not file:/tmp/...
        warehouse_dir = f"gs://{config.gcs_bucket}/spark-warehouse"
        spark_config = builder.config("spark.sql.warehouse.dir", warehouse_dir)
        
        # Packages: spark-xml for CustomerMgmt.xml; delta when --format delta
        packages = ["com.databricks:spark-xml_2.12:0.18.0"]
        if getattr(config, "table_format", None) == "delta":
            packages.append("io.delta:delta-spark_2.12:3.0.0")
        spark_config = spark_config.config("spark.jars.packages", ",".join(packages))
        
        # Configure for GCS
        spark_config = spark_config.config("spark.hadoop.fs.gs.impl", 
                              "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                              "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
                      .config("spark.hadoop.fs.gs.project.id", config.project_id)
        
        # Configure service account authentication if provided.
        # GCS connector expects a *local* key file path; it opens it with FileInputStream.
        # If key file is gs:// or missing, do not set keyfile auth or SparkContext init fails (NPE).
        key_file = getattr(config, "service_account_key_file", None) or ""
        use_keyfile = (
            config.service_account_email
            and key_file
            and not key_file.strip().startswith("gs://")
        )
        if use_keyfile:
            spark_config = spark_config.config("spark.hadoop.fs.gs.auth.type",
                                              "SERVICE_ACCOUNT_JSON_KEYFILE") \
                                      .config("spark.hadoop.fs.gs.auth.service.account.email",
                                              config.service_account_email) \
                                      .config("spark.hadoop.fs.gs.auth.service.account.keyfile",
                                              key_file.strip())
            logger.info("Using service account key file for GCS (local path)")
        elif config.service_account_email:
            spark_config = spark_config.config("spark.hadoop.fs.gs.auth.service.account.email",
                                              config.service_account_email)
            logger.info("Using service account email for GCS (no local key file)")
        
        spark = spark_config.getOrCreate()
        
        logger.info("Created Dataproc SparkSession with GCS support")
        return spark
    
    else:
        raise ValueError(f"Unsupported platform: {config.platform}")


def create_platform_adapter(config: BenchmarkConfig, spark: SparkSession):
    """
    Create platform adapter based on configuration.
    
    Args:
        config: Benchmark configuration
        spark: SparkSession
    
    Returns:
        Platform adapter instance
    """
    if config.platform == Platform.DATABRICKS:
        # Use output_path as raw data input when provided (DBFS or Volume or GCS)
        logger.info(f"[DEBUG create_platform_adapter] config.output_path='{config.output_path}'")
        logger.info(f"[DEBUG create_platform_adapter] config.raw_data_path='{config.raw_data_path}'")

        base = (config.output_path or config.raw_data_path).rstrip("/")
        logger.info(f"[DEBUG create_platform_adapter] base (before normalization)='{base}'")

        # Remove dbfs: prefix from Volume paths if accidentally added
        original_base = base
        if base.startswith("dbfs:/Volumes/"):
            base = base[5:]  # Remove "dbfs:" prefix
            logger.warning(f"[DEBUG create_platform_adapter] Removed 'dbfs:' prefix from Volume path: {original_base} -> {base}")

        # Infer load type from path: dbfs -> DBFS, /Volumes/ -> Volume, gs:// -> GCS (handled by platform)
        raw_root = f"{base}/sf={config.scale_factor}"

        # When reading from GCS on Databricks (classic), set bucket so connector does not throw "No bucket specified in GCS URI: null".
        # On serverless, spark.hadoop.fs.gs.bucket is not available (CONFIG_NOT_AVAILABLE, SQLSTATE: 42K0I). Skip setting it;
        # GCS works via Unity Catalog external locations or the default connector.
        if raw_root.startswith("gs://"):
            bucket_match = re.match(r"gs://([^/]+)", raw_root)
            if bucket_match:
                bucket = bucket_match.group(1)
                try:
                    spark.conf.set("spark.hadoop.fs.gs.bucket", bucket)
                    logger.info(f"Set spark.hadoop.fs.gs.bucket={bucket} for GCS reads on Databricks")
                except BaseException as e:
                    err_msg = str(e).strip()
                    if "CONFIG_NOT_AVAILABLE" in err_msg or "42K0I" in err_msg or "fs.gs.bucket" in err_msg:
                        logger.info(
                            "spark.hadoop.fs.gs.bucket is not available on this runtime (e.g. serverless). "
                            "GCS access will use Unity Catalog external locations or the default connector."
                        )
                    else:
                        logger.warning(
                            f"Could not set spark.hadoop.fs.gs.bucket: {e}. "
                            "GCS access may still work via Unity Catalog external locations or default connector."
                        )

        logger.info(f"[DEBUG create_platform_adapter] Final values:")
        logger.info(f"  base='{base}'")
        logger.info(f"  raw_root='{raw_root}'")

        return DatabricksPlatform(spark, raw_root)
    elif config.platform == Platform.DATAPROC:
        # Build raw_root from base + /sf={scale_factor} (same pattern as Databricks)
        base = config.raw_data_path.rstrip("/")
        base = re.sub(r"/sf=\d+$", "", base) or base  # strip existing /sf=N so scale_factor is source of truth
        raw_root = f"{base}/sf={config.scale_factor}"
        return DataprocPlatform(spark, raw_root,
                               config.gcs_bucket, config.project_id,
                               service_account_email=config.service_account_email,
                               service_account_key_file=config.service_account_key_file,
                               table_format=getattr(config, "table_format", None))
    else:
        raise ValueError(f"Unsupported platform: {config.platform}")


def _get_gcp_machine_type() -> Optional[str]:
    """Get current VM machine type from GCP metadata (e.g. on Dataproc). Returns short name like n2d-standard-16."""
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/instance/machine-type",
            headers={"Metadata-Flavor": "Google"},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            path = resp.read().decode().strip()
            # path is like "projects/123456/machineTypes/n2d-standard-16"
            return path.split("/")[-1] if path else None
    except Exception:
        return None


def _get_dataproc_worker_count_from_metadata() -> Optional[int]:
    """Get number of worker nodes from Dataproc GCP instance metadata (attributes/dataproc-worker-count)."""
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


def _get_executor_count(spark: SparkSession) -> Optional[int]:
    """Get number of executors (worker nodes) from Spark. Excludes driver."""
    try:
        sc = spark.sparkContext
        # getExecutorMemoryStatus gives driver + executors; subtract 1 for driver
        status = sc._jsc.sc().getExecutorMemoryStatus()
        count = status.size() - 1
        return max(0, count) if count is not None else None
    except Exception:
        return None


def is_databricks_serverless(spark: SparkSession) -> bool:
    """
    Heuristic: True if this job appears to be running on Databricks serverless compute.
    
    On serverless, cluster usage tags (e.g. clusterNodeType) are not available
    (CONFIG_NOT_AVAILABLE, SQLSTATE: 42K0I). On classic clusters they are set.
    Use this when you need to branch behavior (e.g. skip unsupported config, log compute type).
    
    Returns:
        True if serverless (or tag read failed), False if classic cluster tags are present.
    """
    try:
        _ = spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType")
        return False  # Config is available -> classic cluster
    except BaseException:
        return True  # Config not available -> likely serverless


def _get_databricks_node_types(spark: SparkSession) -> Tuple[Optional[str], Optional[str]]:
    """
    Get (worker_node_type, driver_node_type) from Databricks Spark conf when available.
    Tries clusterNodeType (worker) and driverNodeType (driver); not all runtimes set both.
    On serverless these configs are not available (CONFIG_NOT_AVAILABLE, SQLSTATE: 42K0I); returns (None, None).
    """
    try:
        worker_type = spark.conf.get("spark.databricks.clusterUsageTags.clusterNodeType")
    except BaseException:
        worker_type = None
    try:
        driver_type = spark.conf.get("spark.databricks.clusterUsageTags.driverNodeType")
    except BaseException:
        driver_type = None
    return (worker_type, driver_type)


def get_cluster_info(config: BenchmarkConfig, spark: SparkSession) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """
    Return (cluster_instance_type, cluster_worker_count, cluster_master_type) from config or auto-detection.
    cluster_instance_type = worker node type; cluster_master_type = driver node type.
    """
    instance_type = getattr(config, "cluster_instance_type", None)
    worker_count = getattr(config, "cluster_worker_count", None)
    master_type = getattr(config, "cluster_master_type", None)

    if config.platform == Platform.DATAPROC:
        # GCP metadata is for the current VM = driver. Use it for driver type; use same for worker if not provided.
        driver_type = _get_gcp_machine_type()
        if master_type is None and driver_type:
            master_type = driver_type
            logger.info(f"Auto-detected cluster_master_type (driver): {master_type}")
        if instance_type is None and driver_type:
            instance_type = driver_type
            logger.info(f"Auto-detected cluster_instance_type (from driver; pass --cluster-instance-type if workers differ): {instance_type}")
        if worker_count is None:
            worker_count = _get_executor_count(spark)
            # Spark executor count can be 0 early or on single-node; use Dataproc metadata when available
            if (worker_count is None or worker_count == 0):
                meta_count = _get_dataproc_worker_count_from_metadata()
                if meta_count is not None:
                    worker_count = meta_count
                    logger.info(f"Auto-detected cluster_worker_count (Dataproc metadata): {worker_count}")
            elif worker_count is not None:
                logger.info(f"Auto-detected cluster_worker_count: {worker_count}")
    elif config.platform == Platform.DATABRICKS:
        worker_type, driver_type = _get_databricks_node_types(spark)
        if instance_type is None and worker_type:
            instance_type = worker_type
            logger.info(f"Auto-detected cluster_instance_type (worker): {instance_type}")
        if master_type is None and driver_type:
            master_type = driver_type
            logger.info(f"Auto-detected cluster_master_type (driver): {master_type}")
        if worker_count is None:
            worker_count = _get_executor_count(spark)
            if worker_count is not None:
                logger.info(f"Auto-detected cluster_worker_count: {worker_count}")

    return (instance_type, worker_count, master_type)


def run_benchmark(config: BenchmarkConfig) -> dict:
    """
    Run TPC-DI benchmark with the given configuration.
    
    Args:
        config: Benchmark configuration
    
    Returns:
        Dictionary with benchmark results and metrics
    """
    logger.info(f"Starting TPC-DI benchmark: {config.platform.value}, "
               f"{config.load_type.value}, SF={config.scale_factor}")
    
    # Create SparkSession
    with MetricsCollector(config) as metrics:
        metrics.start_step("spark_session_creation")
        spark = create_spark_session(config)
        metrics.finish_step()
        metrics.spark = spark  # used for GCS metrics upload when gsutil is not available (e.g. Databricks)
        
        if config.platform == Platform.DATABRICKS:
            serverless = is_databricks_serverless(spark)
            compute_type = "serverless" if serverless else "classic"
            metrics.metrics.databricks_compute_type = compute_type
            logger.info(f"Databricks compute: {compute_type}" + ("" if serverless else " (provisioned)"))
        
        # Create platform adapter
        metrics.start_step("platform_adapter_creation")
        platform = create_platform_adapter(config, spark)
        metrics.finish_step()

        # Set cluster metadata for metrics (from config or auto-detection)
        instance_type, worker_count, master_type = get_cluster_info(config, spark)
        metrics.metrics.set_cluster_info(
            instance_type=instance_type,
            worker_count=worker_count,
            master_type=master_type,
        )

        # Create target database (and catalog/schema for Databricks UC)
        # Append scale factor to database/schema names
        metrics.start_step("database_creation")
        db_name_with_sf = f"{config.target_database}_sf{config.scale_factor}"
        
        # Track if database/path exists before creation (for table override info)
        database_existed = False
        path_existed = False
        
        if config.platform == Platform.DATABRICKS:
            # Databricks requires Unity Catalog (target_catalog is validated in config)
            # For Unity Catalog, append SF to schema name
            schema_name_with_sf = f"{config.target_schema}_sf{config.scale_factor}"
            # Check if schema exists in Unity Catalog
            try:
                schemas = spark.sql(f"SHOW SCHEMAS IN {config.target_catalog}").collect()
                database_existed = any(row.databaseName == schema_name_with_sf for row in schemas)
            except Exception:
                database_existed = False
            
            platform.create_database(
                "",  # Not used for UC
                catalog=config.target_catalog,
                schema=schema_name_with_sf,  # Use schema name with SF
            )
            db_or_catalog = config.target_catalog
            effective_schema = schema_name_with_sf
        elif config.platform == Platform.DATAPROC:
            # spark_catalog expects two-part names (database.table). Use single DB = target_database_target_schema_sf{scale_factor}.
            spark_db = f"{db_name_with_sf}_{config.target_schema}"
            # Check if path exists before deletion
            if config.load_type == LoadType.BATCH:
                path_existed = platform.check_database_path_exists(spark_db)
                platform.delete_target_database_path_if_exists(spark_db)
            else:
                path_existed = platform.check_database_path_exists(spark_db)
            # Check if database exists
            try:
                database_existed = spark.catalog.databaseExists(spark_db)
            except Exception:
                database_existed = False
            platform.create_database(spark_db)
            db_or_catalog = spark_db
            effective_schema = ""
        else:
            raise ValueError(f"Unsupported platform: {config.platform}")
        
        # Store table override info in metrics
        table_override = database_existed or path_existed
        metrics.metrics.table_override = table_override
        metrics.finish_step()
        
        # Run ETL: Medallion only (Bronze -> Silver layers)
        clear_table_timing()
        table_timing_configure(log_detailed_stats=config.log_detailed_stats)
        table_timing_job_start()

        if config.load_type == LoadType.BATCH:
            # Drop all Bronze, Silver, and Gold tables so batch load starts clean
            batch_tables = [
                "bronze_date", "bronze_time", "bronze_status_type", "bronze_tax_rate",
                "bronze_trade_type", "bronze_industry", "bronze_hr", "bronze_customer_mgmt",
                "bronze_trade", "bronze_daily_market", "bronze_prospect", "bronze_cash_transaction",
                "bronze_holding_history", "bronze_watch_history", "bronze_finwire",
                "silver_date", "silver_status_type", "silver_trade_type", "silver_industry",
                "silver_tax_rate", "silver_companies", "silver_securities", "silver_financials",
                "silver_customers", "silver_accounts", "silver_trades", "silver_daily_market",
                "silver_prospect", "silver_cash_transaction", "silver_watch_history", "silver_holding_history",
                "gold_dim_date", "gold_dim_customer", "gold_dim_account", "gold_dim_company",
                "gold_dim_security", "gold_dim_trade_type", "gold_dim_status_type", "gold_dim_industry",
                "gold_financials", "gold_fact_trade", "gold_fact_market_history", "gold_dim_messages",
                "gold_fact_cash_balances", "gold_fact_holdings",
            ]
            if hasattr(platform, "drop_table_if_exists"):
                prefix = ".".join(p for p in (db_or_catalog, effective_schema) if p)
                for short_name in batch_tables:
                    full_name = f"{prefix}.{short_name}" if prefix else short_name
                    try:
                        platform.drop_table_if_exists(full_name)
                    except Exception as e:
                        logger.warning("Could not drop table %s: %s", full_name, e)
            metrics.start_step("bronze_etl")
            from benchmark.etl.bronze import BronzeETL
            bronze_etl = BronzeETL(platform)
            bronze_etl.run_bronze_batch_load(
                1, db_or_catalog, effective_schema,
                use_udtf_customer_mgmt=config.use_udtf_customer_mgmt,
                customer_mgmt_xml_format=getattr(config, "customer_mgmt_xml_format", None),
            )
            
            bronze_tables = ["bronze_customer_mgmt", "bronze_trade", "bronze_daily_market", 
                            "bronze_date", "bronze_status_type", "bronze_trade_type",
                            "bronze_industry", "bronze_finwire"]
            bronze_row_counts = {}
            for table in bronze_tables:
                table_name = ".".join(p for p in (db_or_catalog, effective_schema, table) if p)
                try:
                    bronze_row_counts[table] = platform.get_table_count(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            bronze_bytes = getattr(platform, "get_raw_input_size_bytes", lambda bid: 0)(1)
            metrics.finish_step(rows=sum(bronze_row_counts.values()),
                               bytes=bronze_bytes if bronze_bytes else None,
                               metadata={"table_counts": bronze_row_counts})
            
            metrics.start_step("silver_etl")
            from benchmark.etl.silver import SilverETL
            silver_etl = SilverETL(platform)
            silver_etl.run_silver_batch_load(1, db_or_catalog, effective_schema, metrics=metrics)
            
            silver_tables = ["silver_customers", "silver_accounts", "silver_trades",
                            "silver_daily_market", "silver_date", "silver_status_type",
                            "silver_trade_type", "silver_industry", "silver_companies",
                            "silver_securities", "silver_financials"]
            silver_row_counts = {}
            for table in silver_tables:
                table_name = ".".join(p for p in (db_or_catalog, effective_schema, table) if p)
                try:
                    silver_row_counts[table] = platform.get_table_count(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            metrics.finish_step(rows=sum(silver_row_counts.values()),
                               metadata={"table_counts": silver_row_counts})
            
            # Gold layer: Transform Silver to Gold star schema
            metrics.start_step("gold_etl")
            from benchmark.etl.gold import GoldETL
            gold_etl = GoldETL(platform)
            gold_etl.run_gold_load(db_or_catalog, effective_schema, load_type=config.load_type, batch_id=1)
            
            gold_tables = ["gold_dim_customer", "gold_dim_account", "gold_dim_company",
                          "gold_dim_security", "gold_dim_date", "gold_dim_trade_type",
                          "gold_dim_status_type", "gold_dim_industry",
                          "gold_fact_trade", "gold_fact_market_history"]
            gold_row_counts = {}
            for table in gold_tables:
                table_name = ".".join(p for p in (db_or_catalog, effective_schema, table) if p)
                try:
                    gold_row_counts[table] = platform.get_table_count(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            metrics.finish_step(rows=sum(gold_row_counts.values()),
                               metadata={"table_counts": gold_row_counts})

            table_timing_job_end()
            table_timing_log_final()

        elif config.load_type == LoadType.INCREMENTAL:
            metrics.start_step(f"bronze_incremental_batch{config.batch_id}")
            from benchmark.etl.bronze import BronzeETL
            bronze_etl = BronzeETL(platform)
            bronze_etl.run_bronze_batch_load(
                config.batch_id, db_or_catalog, effective_schema,
                use_udtf_customer_mgmt=config.use_udtf_customer_mgmt,
                customer_mgmt_xml_format=getattr(config, "customer_mgmt_xml_format", None),
            )
            inc_batch_bytes = getattr(platform, "get_raw_input_size_bytes", lambda bid: 0)(config.batch_id)
            metrics.finish_step(bytes=inc_batch_bytes if inc_batch_bytes else None)
            
            metrics.start_step(f"silver_incremental_batch{config.batch_id}")
            from benchmark.etl.silver import SilverETL
            silver_etl = SilverETL(platform)
            silver_etl.run_silver_batch_load(config.batch_id, db_or_catalog, effective_schema, metrics=metrics)
            
            silver_tables = ["silver_customers", "silver_accounts", "silver_trades"]
            row_counts = {}
            for table in silver_tables:
                table_name = ".".join(p for p in (db_or_catalog, effective_schema, table) if p)
                try:
                    row_counts[table] = platform.get_table_count(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            metrics.finish_step(rows=sum(row_counts.values()), metadata={"table_counts": row_counts})
            
            # Gold layer: Refresh Gold tables from updated Silver
            metrics.start_step(f"gold_incremental_batch{config.batch_id}")
            from benchmark.etl.gold import GoldETL
            gold_etl = GoldETL(platform)
            gold_etl.run_gold_load(db_or_catalog, effective_schema, load_type=config.load_type, batch_id=config.batch_id)
            metrics.finish_step()

            table_timing_job_end()
            table_timing_log_final()

        else:
            raise ValueError(f"Unsupported load type: {config.load_type}")
    
    # Cost estimation (compute + software/DBU; list-price approximation)
    try:
        from benchmark.cost import estimate_cost
        cost = estimate_cost(
            metrics.metrics,
            config.platform.value,
            getattr(config, "cloud", None),
        )
        if cost:
            metrics.metrics.cost_breakdown = {
                "compute_usd": cost.get("compute_usd", 0),
                "software_usd": cost.get("software_usd", cost.get("dbu_usd", 0)),
            }
            metrics.metrics.total_cost_usd = cost.get("total_usd")
            if config.platform == Platform.DATABRICKS and cost.get("dbu_usd") is not None:
                metrics.metrics.dbu_cost_usd = cost.get("dbu_usd")
    except Exception as e:
        logger.debug("Cost estimation skipped: %s", e)
    
    logger.info("Benchmark completed successfully")
    return {
        "status": "success",
        "metrics": metrics.metrics.to_dict(),
        "config": {
            "platform": config.platform.value,
            "load_type": config.load_type.value,
            "scale_factor": config.scale_factor,
            "batch_id": config.batch_id,
        }
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Run TPC-DI benchmark")
    parser.add_argument("--platform", choices=["databricks", "dataproc"], required=True)
    parser.add_argument("--load-type", choices=["batch", "incremental"], required=True)
    parser.add_argument("--scale-factor", type=int, required=True)
    parser.add_argument("--raw-data-path", help="GCS path for Dataproc; base path for Databricks if --output-path not set")
    parser.add_argument("--output-path", help="Databricks: raw data location (DBFS or Volume base); overrides raw-data-path")
    parser.add_argument("--target-database", default="tpcdi_warehouse")
    parser.add_argument("--target-schema", default="dw")
    parser.add_argument("--target-catalog", help="Unity Catalog (Databricks); when set, create catalog + schema")
    parser.add_argument("--batch-id", type=int, help="Required for incremental loads")
    parser.add_argument("--gcs-bucket", help="Required for Dataproc")
    parser.add_argument("--project-id", help="Required for Dataproc")
    parser.add_argument("--region", help="Required for Dataproc")
    parser.add_argument("--spark-master", help="Spark master URL for Dataproc")
    parser.add_argument("--metrics-output", help="Path to save metrics JSON")
    parser.add_argument("--log-detailed-stats", action="store_true",
                        help="Log per-table timing and records; default is only job start/end/total duration")
    parser.add_argument("--use-udtf-customer-mgmt", choices=["auto", "true", "false"], default="false",
                        help="CustomerMgmt.xml: auto=UDTF on Databricks, true=UDTF, false=spark-xml")
    
    args = parser.parse_args()
    
    # Databricks: use output_path as raw data input when set; else raw_data_path
    raw_base = args.output_path or args.raw_data_path
    if not raw_base and args.platform == "dataproc":
        raw_base = args.raw_data_path
    if not raw_base:
        raise ValueError("Provide --raw-data-path or --output-path (Databricks)")
    
    use_udtf = {"auto": None, "true": True, "false": False}[args.use_udtf_customer_mgmt]
    config = BenchmarkConfig(
        platform=Platform(args.platform),
        load_type=LoadType(args.load_type),
        scale_factor=args.scale_factor,
        raw_data_path=raw_base,
        target_database=args.target_database,
        target_schema=args.target_schema,
        target_catalog=args.target_catalog,
        output_path=args.output_path,
        batch_id=args.batch_id,
        gcs_bucket=args.gcs_bucket,
        project_id=args.project_id,
        region=args.region,
        spark_master=args.spark_master,
        metrics_output_path=args.metrics_output,
        log_detailed_stats=args.log_detailed_stats,
        use_udtf_customer_mgmt=use_udtf,
    )
    
    result = run_benchmark(config)
    print(f"\nBenchmark Results:\n{result}")
