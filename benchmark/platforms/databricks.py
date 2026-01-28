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
    """Platform adapter for Databricks. Reads raw data from DBFS or Unity Catalog Volume."""
    
    def __init__(self, spark: SparkSession, raw_data_path: str, use_volume: bool = False):
        """
        Initialize Databricks platform adapter.
        
        Args:
            spark: SparkSession (should already be configured)
            raw_data_path: Base path to raw TPC-DI data including sf=X
                          (e.g. dbfs:/mnt/tpcdi/sf=10 or /Volumes/cat/schema/vol/sf=10)
            use_volume: True if raw data is in a Unity Catalog Volume
        """
        logger.info(f"[DEBUG DatabricksPlatform.__init__] Called with:")
        logger.info(f"  raw_data_path='{raw_data_path}'")
        logger.info(f"  use_volume={use_volume}")
        
        self.spark = spark
        # Normalize path: remove dbfs: prefix from Volume paths
        normalized = raw_data_path.rstrip("/")
        logger.info(f"[DEBUG DatabricksPlatform.__init__] normalized (before checks)='{normalized}'")
        
        if normalized.startswith("dbfs:/Volumes/"):
            # Volume path incorrectly prefixed with dbfs: - remove it
            original = normalized
            normalized = normalized[5:]  # Remove "dbfs:"
            logger.warning(
                f"[DEBUG DatabricksPlatform.__init__] Removed 'dbfs:' prefix from Volume path: {original} -> {normalized}"
            )
        elif normalized.startswith("/Volumes/"):
            # Volume path is correct
            logger.info(f"[DEBUG DatabricksPlatform.__init__] Volume path detected (starts with /Volumes/)")
        elif use_volume and not normalized.startswith("/Volumes/"):
            # use_volume=True but path doesn't start with /Volumes/
            logger.warning(
                f"[DEBUG DatabricksPlatform.__init__] use_volume=True but path doesn't start with /Volumes/: {normalized}"
            )
        
        self.raw_data_path = normalized
        self.use_volume = use_volume
        logger.info(
            f"[DEBUG DatabricksPlatform.__init__] Final values:"
        )
        logger.info(
            f"  self.raw_data_path='{self.raw_data_path}'"
        )
        logger.info(
            f"  self.use_volume={self.use_volume}"
        )
    
    def _resolve_path(self, relative_path: str) -> str:
        """Resolve full path for reading. Handles both DBFS and Volume."""
        logger.debug(f"[DEBUG _resolve_path] Input: relative_path='{relative_path}', self.raw_data_path='{self.raw_data_path}'")
        full = f"{self.raw_data_path}/{relative_path}".replace("//", "/")
        logger.debug(f"[DEBUG _resolve_path] After concatenation: '{full}'")
        # Ensure Volume paths never get dbfs: prefix
        if full.startswith("dbfs:/Volumes/"):
            original_full = full
            full = full[5:]  # Remove "dbfs:" prefix
            logger.warning(f"[DEBUG _resolve_path] Removed 'dbfs:' prefix: {original_full} -> {full}")
        logger.debug(f"[DEBUG _resolve_path] Final resolved path: '{full}'")
        return full
    
    def read_raw_file(self, file_path: str, schema: Optional[StructType] = None, 
                     format: str = "csv", **options) -> DataFrame:
        """
        Read a raw data file from DBFS or Unity Catalog Volume.
        
        Args:
            file_path: Relative path from raw_data_path (e.g., "Batch1/CustomerMgmt.txt")
            schema: Optional schema for the data
            format: File format (csv, parquet, json, etc.)
            **options: Additional options for the reader (e.g., delimiter, header)
        
        Returns:
            DataFrame with the data
        """
        logger.info(f"[DEBUG read_raw_file] Called with:")
        logger.info(f"  file_path='{file_path}'")
        logger.info(f"  self.raw_data_path='{self.raw_data_path}'")
        logger.info(f"  self.use_volume={self.use_volume}")
        
        full_path = self._resolve_path(file_path)
        logger.info(f"[DEBUG read_raw_file] After _resolve_path: '{full_path}'")
        
        # Final safety check: ensure Volume paths never have dbfs: prefix
        if full_path.startswith("dbfs:/Volumes/"):
            original_full_path = full_path
            full_path = full_path[5:]
            logger.warning(f"[DEBUG read_raw_file] Removed 'dbfs:' prefix in read_raw_file: {original_full_path} -> {full_path}")
        
        logger.info(f"[DEBUG read_raw_file] FINAL PATH TO READ: '{full_path}'")
        logger.info(f"[DEBUG read_raw_file] About to call spark.read.format('{format}').load('{full_path}')")
        
        reader = self.spark.read.format(format)
        if schema:
            reader = reader.schema(schema)
        
        for key, value in options.items():
            reader = reader.option(key, value)
        
        logger.info(f"[DEBUG read_raw_file] Calling reader.load('{full_path}')...")
        result = reader.load(full_path)
        logger.info(f"[DEBUG read_raw_file] Successfully loaded file: '{full_path}'")
        return result
    
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
        logger.info(f"[DEBUG read_batch_files] Called with:")
        logger.info(f"  batch_id={batch_id}")
        logger.info(f"  file_pattern='{file_pattern}'")
        logger.info(f"  self.raw_data_path='{self.raw_data_path}'")
        batch_path = f"Batch{batch_id}/{file_pattern}"
        logger.info(f"[DEBUG read_batch_files] Constructed batch_path='{batch_path}'")
        logger.info(f"[DEBUG read_batch_files] Calling read_raw_file('{batch_path}')...")
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
        logger.info(f"[DEBUG read_historical_files] Called with:")
        logger.info(f"  file_pattern='{file_pattern}'")
        logger.info(f"  self.raw_data_path='{self.raw_data_path}'")
        hist_path = f"HistoricalLoad/{file_pattern}"
        logger.info(f"[DEBUG read_historical_files] Constructed hist_path='{hist_path}'")
        logger.info(f"[DEBUG read_historical_files] Calling read_raw_file('{hist_path}')...")
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
