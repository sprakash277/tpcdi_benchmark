"""
Databricks platform adapter for TPC-DI benchmark.
Handles DBFS paths and Databricks-specific Spark configuration.
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DatabricksPlatform:
    """Platform adapter for Databricks with DBFS storage."""
    
    def __init__(self, spark: SparkSession, raw_data_path: str):
        """
        Initialize Databricks platform adapter.
        
        Args:
            spark: SparkSession (should already be configured)
            raw_data_path: Base path to raw TPC-DI data in DBFS (e.g., dbfs:/mnt/tpcdi/sf=10)
        """
        self.spark = spark
        self.raw_data_path = raw_data_path.rstrip("/")
        logger.info(f"Initialized Databricks platform with raw_data_path: {self.raw_data_path}")
    
    def read_raw_file(self, file_path: str, schema: Optional[StructType] = None, 
                     format: str = "csv", **options) -> DataFrame:
        """
        Read a raw data file from DBFS.
        
        Args:
            file_path: Relative path from raw_data_path (e.g., "Batch1/CustomerMgmt.txt")
            schema: Optional schema for the data
            format: File format (csv, parquet, json, etc.)
            **options: Additional options for the reader (e.g., delimiter, header)
        
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
        Read files from the HistoricalLoad directory.
        
        Args:
            file_pattern: File pattern (e.g., "HR.csv")
            schema: Optional schema
            **options: Reader options
        
        Returns:
            DataFrame with the data
        """
        hist_path = f"HistoricalLoad/{file_pattern}"
        return self.read_raw_file(hist_path, schema=schema, **options)
    
    def write_table(self, df: DataFrame, table_name: str, mode: str = "overwrite",
                   partition_by: Optional[list] = None, format: str = "delta"):
        """
        Write DataFrame to a table in the target database.
        
        Args:
            df: DataFrame to write
            table_name: Target table name (will be written to database.schema.table)
            mode: Write mode (overwrite, append, etc.)
            partition_by: Optional list of columns to partition by
            format: Table format (delta, parquet, etc.)
        """
        logger.info(f"Writing table: {table_name} (mode={mode}, format={format})")
        
        writer = df.write.format(format).mode(mode)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        
        writer.saveAsTable(table_name)
    
    def create_database(
        self,
        database_name: str,
        if_not_exists: bool = True,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        """
        Create a database (and optionally catalog + schema for Unity Catalog).

        When catalog and schema are provided (Unity Catalog):
          - CREATE CATALOG IF NOT EXISTS catalog
          - CREATE SCHEMA IF NOT EXISTS catalog.schema

        Otherwise (Hive metastore):
          - CREATE DATABASE IF NOT EXISTS database_name

        Args:
            database_name: Database name (used when catalog/schema not provided).
            if_not_exists: If True, use IF NOT EXISTS.
            catalog: Optional Unity Catalog name.
            schema: Optional schema name (used with catalog).
        """
        exists_clause = "IF NOT EXISTS" if if_not_exists else ""
        if catalog and schema:
            self.spark.sql(f"CREATE CATALOG {exists_clause} {catalog}")
            self.spark.sql(f"CREATE SCHEMA {exists_clause} {catalog}.{schema}")
            logger.info(f"Created catalog {catalog} and schema {catalog}.{schema}")
        else:
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
            result = self.spark.sql(
                f"SELECT SUM(size) / (1024 * 1024) as size_mb "
                f"FROM (SELECT size FROM DESCRIBE DETAIL {table_name})"
            ).first()
            return result.size_mb if result and result.size_mb else 0.0
        except Exception as e:
            logger.warning(f"Could not get table size: {e}")
            return 0.0
