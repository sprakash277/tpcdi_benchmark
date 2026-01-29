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
    """Platform adapter for Databricks. Reads raw data from DBFS or Unity Catalog Volume.
    Load type is inferred from path: dbfs:/... -> DBFS, /Volumes/... -> Volume.
    """

    def __init__(self, spark: SparkSession, raw_data_path: str):
        """
        Initialize Databricks platform adapter.

        Args:
            spark: SparkSession (should already be configured)
            raw_data_path: Base path to raw TPC-DI data including sf=X
                          (e.g. dbfs:/mnt/tpcdi/sf=10 or /Volumes/cat/schema/vol/sf=10)
        """
        logger.info(f"[DEBUG DatabricksPlatform.__init__] Called with:")
        logger.info(f"  raw_data_path='{raw_data_path}'")

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

        self.raw_data_path = normalized
        # Infer from path: /Volumes/ -> Volume load, otherwise DBFS
        self.use_volume = normalized.startswith("/Volumes/")
        logger.info(f"[DEBUG DatabricksPlatform.__init__] Final: raw_data_path='{self.raw_data_path}', use_volume={self.use_volume}")
    
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
        logger.info(f"[DEBUG read_raw_file] Options: {options}")
        logger.info(f"[DEBUG read_raw_file] Format: {format}, use_volume: {self.use_volume}")
        
        # For Unity Catalog Volumes, Spark needs the path as-is without dbfs: prefix
        # Spark will automatically handle Volume paths when they start with /Volumes/
        reader = self.spark.read.format(format)
        if schema:
            reader = reader.schema(schema)
        
        # Apply options - ensure delimiter/sep is properly set
        for key, value in options.items():
            logger.debug(f"[DEBUG read_raw_file] Setting option: {key}={value} (type: {type(value).__name__})")
            reader = reader.option(key, value)
        
        # If sep is provided but delimiter is not, also set delimiter (some Spark versions need both)
        if "sep" in options and "delimiter" not in options:
            sep_value = options["sep"]
            logger.info(f"[DEBUG read_raw_file] Also setting delimiter={sep_value} (same as sep)")
            reader = reader.option("delimiter", sep_value)
        elif "delimiter" in options and "sep" not in options:
            # If delimiter is set but sep is not, also set sep
            delim_value = options["delimiter"]
            logger.info(f"[DEBUG read_raw_file] Also setting sep={delim_value} (same as delimiter)")
            reader = reader.option("sep", delim_value)
        
        # For Volume paths, ensure Spark doesn't add dbfs: prefix
        # Spark should recognize /Volumes/ paths automatically
        logger.info(f"[DEBUG read_raw_file] Calling reader.load('{full_path}')...")
        
        # Verify file exists for Volume paths (helps debug path issues)
        if full_path.startswith("/Volumes/"):
            try:
                # Try to verify path exists using dbutils if available
                import dbutils  # type: ignore
                files = dbutils.fs.ls(full_path.rsplit("/", 1)[0])  # List parent directory
                logger.info(f"[DEBUG read_raw_file] Verified Volume directory exists: {full_path.rsplit('/', 1)[0]}")
            except Exception as verify_error:
                logger.warning(f"[DEBUG read_raw_file] Could not verify Volume path: {verify_error}")
        
        try:
            result = reader.load(full_path)
        except Exception as e:
            error_msg = str(e)
            # Check if Spark added dbfs: prefix to Volume path
            if "dbfs:/Volumes/" in error_msg and full_path.startswith("/Volumes/"):
                logger.error(
                    f"[DEBUG read_raw_file] Spark added 'dbfs:' prefix to Volume path! "
                    f"Original path: '{full_path}', Error: {error_msg}"
                )
                logger.error(
                    f"[DEBUG read_raw_file] This is a known issue with some Spark/Databricks versions. "
                    f"Unity Catalog Volumes should be accessed directly with /Volumes/ paths."
                )
                # Provide helpful error message
                raise RuntimeError(
                    f"Spark is adding 'dbfs:' prefix to Volume path '{full_path}'. "
                    f"This prevents reading from Unity Catalog Volumes. "
                    f"Possible solutions:\n"
                    f"1. Ensure your Databricks runtime supports Unity Catalog Volumes\n"
                    f"2. Check Spark configuration for path resolution settings\n"
                    f"3. Verify the file exists at: {full_path}\n"
                    f"Original error: {error_msg}"
                ) from e
            raise
        
        logger.info(f"[DEBUG read_raw_file] Successfully loaded file: '{full_path}'")
        logger.info(f"[DEBUG read_raw_file] Result has {len(result.columns)} columns: {result.columns}")
        
        # If we only got 1 column but delimiter was set, log a warning with sample data
        if len(result.columns) == 1 and ("delimiter" in options or "sep" in options) and format == "csv":
            logger.warning(f"[DEBUG read_raw_file] WARNING: Only got 1 column despite delimiter being set!")
            logger.warning(f"[DEBUG read_raw_file] Delimiter was: {options.get('delimiter', options.get('sep', 'NOT SET'))}")
            logger.warning(f"[DEBUG read_raw_file] Sample row: {result.first()}")
        
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
        logger.info(f"[DEBUG read_historical_files] Called with:")
        logger.info(f"  file_pattern='{file_pattern}'")
        logger.info(f"  self.raw_data_path='{self.raw_data_path}'")
        logger.info(f"[DEBUG read_historical_files] Redirecting to read_batch_files(1, '{file_pattern}') per TPC-DI spec")
        # TPC-DI spec: historical data is in Batch1, not HistoricalLoad
        return self.read_batch_files(1, file_pattern, schema=schema, **options)
    
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
        
        # Check if table exists
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(table_name)
        except Exception as e:
            logger.warning(f"Could not check if table {table_name} exists: {e}")
        
        # For Delta Lake with overwrite mode, drop table first to avoid schema merge conflicts
        if mode == "overwrite" and format == "delta" and table_exists:
            try:
                logger.info(f"Table {table_name} exists. Dropping it before overwrite to avoid schema conflicts.")
                self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                logger.warning(f"Could not drop table {table_name}: {e}. Proceeding with write.")
        
        # For append mode, if table doesn't exist, create it (treat as overwrite for first write)
        actual_mode = mode
        if mode == "append" and not table_exists:
            logger.info(f"Table {table_name} does not exist. Creating it with first write.")
            actual_mode = "overwrite"
        
        writer = df.write.format(format).mode(actual_mode)
        
        # For Delta append, enable schema merge in case of minor schema differences
        if format == "delta" and mode == "append" and table_exists:
            writer = writer.option("mergeSchema", "true")
        
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
