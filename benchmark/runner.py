"""
Main benchmark runner for TPC-DI benchmark.
Orchestrates ETL execution on Databricks or Dataproc platforms.
"""

import logging
from typing import Optional
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
        # Use output_path as raw data input when provided (DBFS or Volume)
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
        
        logger.info(f"[DEBUG create_platform_adapter] Final values:")
        logger.info(f"  base='{base}'")
        logger.info(f"  raw_root='{raw_root}'")
        
        return DatabricksPlatform(spark, raw_root)
    elif config.platform == Platform.DATAPROC:
        return DataprocPlatform(spark, config.raw_data_path, 
                               config.gcs_bucket, config.project_id,
                               service_account_email=config.service_account_email,
                               service_account_key_file=config.service_account_key_file,
                               table_format=getattr(config, "table_format", None))
    else:
        raise ValueError(f"Unsupported platform: {config.platform}")


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
        
        # Create platform adapter
        metrics.start_step("platform_adapter_creation")
        platform = create_platform_adapter(config, spark)
        metrics.finish_step()
        
        # Create target database (and catalog/schema for Databricks UC when configured)
        metrics.start_step("database_creation")
        if config.platform == Platform.DATABRICKS and config.target_catalog:
            platform.create_database(
                config.target_database,
                catalog=config.target_catalog,
                schema=config.target_schema,
            )
            db_or_catalog = config.target_catalog
            effective_schema = config.target_schema
        elif config.platform == Platform.DATAPROC:
            # spark_catalog expects two-part names (database.table). Use single DB = target_database_target_schema.
            spark_db = f"{config.target_database}_{config.target_schema}"
            platform.create_database(spark_db)
            db_or_catalog = spark_db
            effective_schema = ""
        else:
            platform.create_database(config.target_database)
            db_or_catalog = config.target_database
            effective_schema = config.target_schema
        metrics.finish_step()
        
        # Run ETL: Medallion only (Bronze -> Silver layers)
        clear_table_timing()
        table_timing_configure(log_detailed_stats=config.log_detailed_stats)
        table_timing_job_start()

        if config.load_type == LoadType.BATCH:
            metrics.start_step("bronze_etl")
            from benchmark.etl.bronze import BronzeETL
            bronze_etl = BronzeETL(platform)
            bronze_etl.run_bronze_batch_load(1, db_or_catalog, effective_schema)
            
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
            metrics.finish_step(rows=sum(bronze_row_counts.values()), 
                               metadata={"table_counts": bronze_row_counts})
            
            metrics.start_step("silver_etl")
            from benchmark.etl.silver import SilverETL
            silver_etl = SilverETL(platform)
            silver_etl.run_silver_batch_load(1, db_or_catalog, effective_schema)
            
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
            gold_etl.run_gold_load(db_or_catalog, effective_schema)
            
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
            bronze_etl.run_bronze_batch_load(config.batch_id, db_or_catalog, effective_schema)
            metrics.finish_step()
            
            metrics.start_step(f"silver_incremental_batch{config.batch_id}")
            from benchmark.etl.silver import SilverETL
            silver_etl = SilverETL(platform)
            silver_etl.run_silver_batch_load(config.batch_id, db_or_catalog, effective_schema)
            
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
            gold_etl.run_gold_load(db_or_catalog, effective_schema)
            metrics.finish_step()

            table_timing_job_end()
            table_timing_log_final()

        else:
            raise ValueError(f"Unsupported load type: {config.load_type}")
    
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
    
    args = parser.parse_args()
    
    # Databricks: use output_path as raw data input when set; else raw_data_path
    raw_base = args.output_path or args.raw_data_path
    if not raw_base and args.platform == "dataproc":
        raw_base = args.raw_data_path
    if not raw_base:
        raise ValueError("Provide --raw-data-path or --output-path (Databricks)")
    
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
    )
    
    result = run_benchmark(config)
    print(f"\nBenchmark Results:\n{result}")
