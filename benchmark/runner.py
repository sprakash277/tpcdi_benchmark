"""
Main benchmark runner for TPC-DI benchmark.
Orchestrates ETL execution on Databricks or Dataproc platforms.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession

from benchmark.config import BenchmarkConfig, Platform, LoadType
from benchmark.metrics import MetricsCollector
from benchmark.platforms.databricks import DatabricksPlatform
from benchmark.platforms.dataproc import DataprocPlatform
from benchmark.etl.batch import BatchETL
from benchmark.etl.incremental import IncrementalETL

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
        
        # Configure for GCS
        spark = builder.config("spark.hadoop.fs.gs.impl", 
                              "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
                      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                              "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
                      .config("spark.hadoop.fs.gs.project.id", config.project_id) \
                      .getOrCreate()
        
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
        base = (config.output_path or config.raw_data_path).rstrip("/")
        raw_root = f"{base}/sf={config.scale_factor}"
        return DatabricksPlatform(spark, raw_root, use_volume=config.use_volume)
    elif config.platform == Platform.DATAPROC:
        return DataprocPlatform(spark, config.raw_data_path, 
                               config.gcs_bucket, config.project_id)
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
        else:
            platform.create_database(config.target_database)
            db_or_catalog = config.target_database
        metrics.finish_step()
        
        # Run ETL based on load type (use catalog.schema or database.schema for table names)
        if config.load_type == LoadType.BATCH:
            metrics.start_step("batch_etl")
            etl = BatchETL(platform)
            etl.run_full_batch_load(db_or_catalog, config.target_schema)
            
            # Collect metrics
            row_counts = {}
            table_sizes = {}
            for table in ["DimDate", "DimTime", "DimTradeType", "DimStatusType", 
                         "DimTaxRate", "DimIndustry", "DimAccount"]:
                table_name = f"{db_or_catalog}.{config.target_schema}.{table}"
                try:
                    row_counts[table] = platform.get_table_count(table_name)
                    table_sizes[table] = platform.get_table_size_mb(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            
            total_rows = sum(row_counts.values())
            total_size_mb = sum(table_sizes.values())
            metrics.finish_step(rows=total_rows, bytes=int(total_size_mb * 1024 * 1024),
                              metadata={"table_counts": row_counts, "table_sizes_mb": table_sizes})
        
        elif config.load_type == LoadType.INCREMENTAL:
            metrics.start_step("incremental_etl")
            etl = IncrementalETL(platform)
            etl.process_batch(config.batch_id, db_or_catalog, config.target_schema)
            
            # Collect metrics
            row_counts = {}
            for table in ["DimAccount", "FactTrade", "DimCustomer"]:
                table_name = f"{db_or_catalog}.{config.target_schema}.{table}"
                try:
                    row_counts[table] = platform.get_table_count(table_name)
                except Exception as e:
                    logger.warning(f"Could not get metrics for {table}: {e}")
            
            total_rows = sum(row_counts.values())
            metrics.finish_step(rows=total_rows, metadata={"table_counts": row_counts})
        
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
    parser.add_argument("--use-volume", action="store_true", help="Databricks: raw data in Unity Catalog Volume")
    parser.add_argument("--target-database", default="tpcdi_warehouse")
    parser.add_argument("--target-schema", default="dw")
    parser.add_argument("--target-catalog", help="Unity Catalog (Databricks); when set, create catalog + schema")
    parser.add_argument("--batch-id", type=int, help="Required for incremental loads")
    parser.add_argument("--gcs-bucket", help="Required for Dataproc")
    parser.add_argument("--project-id", help="Required for Dataproc")
    parser.add_argument("--region", help="Required for Dataproc")
    parser.add_argument("--spark-master", help="Spark master URL for Dataproc")
    parser.add_argument("--metrics-output", help="Path to save metrics JSON")
    
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
        use_volume=args.use_volume,
        batch_id=args.batch_id,
        gcs_bucket=args.gcs_bucket,
        project_id=args.project_id,
        region=args.region,
        spark_master=args.spark_master,
        metrics_output_path=args.metrics_output,
    )
    
    result = run_benchmark(config)
    print(f"\nBenchmark Results:\n{result}")
