"""
Dataproc platform adapter for TPC-DI benchmark.
Handles GCS paths and Dataproc-specific Spark configuration.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DataprocPlatform:
    """Platform adapter for Dataproc with GCS storage."""
    
    def __init__(self, spark: SparkSession, raw_data_path: str, 
                 gcs_bucket: str, project_id: str):
        """
        Initialize Dataproc platform adapter.
        
        Args:
            spark: SparkSession (should already be configured)
            raw_data_path: Base path to raw TPC-DI data in GCS (e.g., gs://bucket/tpcdi/sf=10)
            gcs_bucket: GCS bucket name
            project_id: GCP project ID
        """
        self.spark = spark
        self.raw_data_path = raw_data_path.rstrip("/")
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        
        # Ensure GCS connector is available
        try:
            # Set Hadoop configuration for GCS
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
            )
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.AbstractFileSystem.gs.impl", 
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
            )
            spark.sparkContext._jsc.hadoopConfiguration().set(
                "fs.gs.project.id", project_id
            )
        except Exception as e:
            logger.warning(f"Could not configure GCS connector: {e}")
        
        logger.info(f"Initialized Dataproc platform with raw_data_path: {self.raw_data_path}")
    
    def read_raw_file(self, file_path: str, schema: Optional[StructType] = None,
                     format: str = "csv", **options) -> DataFrame:
        """
        Read a raw data file from GCS.
        
        Args:
            file_path: Relative path from raw_data_path (e.g., "Batch1/CustomerMgmt.txt")
            schema: Optional schema for the data
            format: File format (csv, parquet, json, etc.)
            **options: Additional options for the reader
        
        Returns:
            DataFrame with the data
        """
        full_path = f"{self.raw_data_path}/{file_path}"
        logger.debug(f"Reading file: {full_path}")
        
        reader = self.spark.read.format(format)
        if schema:
            reader = reader.schema(schema)
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        return reader.load(full_path)
    
    def read_batch_files(self, batch_id: int, file_pattern: str,
                        schema: Optional[StructType] = None, **options) -> DataFrame:
        """
        Read files from a specific batch directory.
        
        Args:
            batch_id: Batch number (e.g., 1, 2, 3...)
            file_pattern: File pattern within batch (e.g., "CustomerMgmt*.txt")
            schema: Optional schema
            **options: Reader options
        
        Returns:
            DataFrame with the data
        """
        batch_path = f"Batch{batch_id}/{file_pattern}"
        return self.read_raw_file(batch_path, schema=schema, **options)
    
    def read_historical_files(self, file_pattern: str, 
                             schema: Optional[StructType] = None, **options) -> DataFrame:
        """
        Read files from Batch1 directory (TPC-DI spec: historical data is in Batch1).
        
        Note: This method is kept for backward compatibility. New code should use
        read_batch_files(1, file_pattern, ...) directly.
        
        Args:
            file_pattern: File pattern (e.g., "Date.txt")
            schema: Optional schema
            **options: Reader options
        
        Returns:
            DataFrame with the data
        """
        # TPC-DI spec: historical data is in Batch1, not HistoricalLoad
        return self.read_batch_files(1, file_pattern, schema=schema, **options)
    
    def write_table(self, df: DataFrame, table_name: str, mode: str = "overwrite",
                   partition_by: Optional[list] = None, format: str = "parquet"):
        """
        Write DataFrame to a table in the target database.
        For Dataproc, we typically use Parquet format stored in GCS.
        
        Args:
            df: DataFrame to write
            table_name: Target table name (database.schema.table)
            mode: Write mode (overwrite, append, etc.)
            partition_by: Optional list of columns to partition by
            format: Table format (parquet, delta, etc.)
        """
        logger.info(f"Writing table: {table_name} (mode={mode}, format={format})")
        
        # For Dataproc, we might write to GCS-backed Hive tables
        # or use Spark SQL to create managed tables
        writer = df.write.format(format).mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.saveAsTable(table_name)
    
    def create_database(self, database_name: str, if_not_exists: bool = True):
        """Create a database if it doesn't exist."""
        exists_clause = "IF NOT EXISTS" if if_not_exists else ""
        self.spark.sql(f"CREATE DATABASE {exists_clause} {database_name}")
        logger.info(f"Created database: {database_name}")
    
    def get_spark(self) -> SparkSession:
        """Get the SparkSession."""
        return self.spark
    
    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table."""
        result = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").first()
        return result.cnt if result else 0
    
    def get_table_size_mb(self, table_name: str) -> float:
        """Get approximate table size in MB."""
        try:
            # For Parquet tables, we can check the file sizes
            # This is a simplified version - actual implementation may vary
            result = self.spark.sql(
                f"SELECT SUM(size) / (1024 * 1024) as size_mb "
                f"FROM (SELECT size FROM DESCRIBE DETAIL {table_name})"
            ).first()
            return result.size_mb if result and result.size_mb else 0.0
        except Exception as e:
            logger.warning(f"Could not get table size: {e}")
            return 0.0
