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
                 gcs_bucket: str, project_id: str,
                 service_account_email: Optional[str] = None,
                 service_account_key_file: Optional[str] = None,
                 table_format: Optional[str] = None):
        """
        Initialize Dataproc platform adapter.
        
        Args:
            spark: SparkSession (should already be configured)
            raw_data_path: Base path to raw TPC-DI data in GCS (e.g., gs://bucket/tpcdi/sf=10)
            gcs_bucket: GCS bucket name
            project_id: GCP project ID
            service_account_email: Optional service account email for GCS access
            service_account_key_file: Optional path to service account JSON key file
            table_format: Table format (delta or parquet); default parquet when None
        """
        self.spark = spark
        self.raw_data_path = raw_data_path.rstrip("/")
        self.gcs_bucket = gcs_bucket
        self.project_id = project_id
        self.table_format = (table_format or "parquet").lower()
        
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
            
            # Configure service account authentication if provided.
            # GCS connector expects a local key file path (FileInputStream); gs:// path causes NPE.
            key_file = (service_account_key_file or "").strip()
            use_keyfile = (
                service_account_email
                and key_file
                and not key_file.startswith("gs://")
            )
            if use_keyfile:
                spark.sparkContext._jsc.hadoopConfiguration().set(
                    "fs.gs.auth.type", "SERVICE_ACCOUNT_JSON_KEYFILE"
                )
                spark.sparkContext._jsc.hadoopConfiguration().set(
                    "fs.gs.auth.service.account.email", service_account_email
                )
                spark.sparkContext._jsc.hadoopConfiguration().set(
                    "fs.gs.auth.service.account.keyfile", key_file
                )
                logger.info(f"Configured GCS access with service account key file: {service_account_email}")
            elif service_account_email:
                # Use service account email only (assumes cluster has access to impersonate)
                spark.sparkContext._jsc.hadoopConfiguration().set(
                    "fs.gs.auth.service.account.email", service_account_email
                )
                logger.info(f"Configured GCS access with service account email: {service_account_email}")
            else:
                # Use default authentication (cluster's service account or Application Default Credentials)
                logger.info("Using default GCS authentication (cluster service account or ADC)")
        except Exception as e:
            logger.warning(f"Could not configure GCS connector: {e}")
        
        logger.info(f"Initialized Dataproc platform with raw_data_path: {self.raw_data_path}")

    def _resolve_path(self, relative_path: str) -> str:
        """Resolve relative path against raw_data_path (e.g. gs://bucket/tpcdi/sf=10)."""
        return f"{self.raw_data_path}/{relative_path}".replace("//", "/")
    
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
                   partition_by: Optional[list] = None, format: Optional[str] = None):
        """
        Write DataFrame to a table in the target database.
        Uses --format (delta | parquet) from config; default parquet.
        Use delta only if Delta package is on cluster (e.g. --packages io.delta:delta-spark_2.12:3.0.0).
        
        Args:
            df: DataFrame to write
            table_name: Target table name (database.table on Dataproc)
            mode: Write mode (overwrite, append, etc.)
            partition_by: Optional list of columns to partition by
            format: Override table format (delta | parquet); when None, use platform default from config
        """
        fmt = (format or self.table_format).lower()
        logger.info(f"Writing table: {table_name} (mode={mode}, format={fmt})")
        
        # Check if table exists
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(table_name)
        except Exception as e:
            logger.warning(f"Could not check if table {table_name} exists: {e}")
        
        # For Delta Lake with overwrite mode, drop table first to avoid schema merge conflicts
        if mode == "overwrite" and fmt == "delta" and table_exists:
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
        
        writer = df.write.format(fmt).mode(actual_mode)
        
        # For Delta append, enable schema merge in case of minor schema differences
        if fmt == "delta" and mode == "append" and table_exists:
            writer = writer.option("mergeSchema", "true")
        
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
        """Get approximate table size in MB. Uses table location + Hadoop FS to sum file sizes.
        Skips DESCRIBE DETAIL because Dataproc's Delta/Spark can raise PARSE_SYNTAX_ERROR when
        rewriting it to SELECT SUM(size) FROM (SELECT ... DESCRIBE DETAIL ...).
        Uses DESCRIBE EXTENDED (standard Spark SQL) to get table location."""
        try:
            desc_df = self.spark.sql(f"DESCRIBE EXTENDED {table_name}")
            loc_row = desc_df.filter("col_name = 'Location'").first()
            if loc_row is None:
                return 0.0
            location = getattr(loc_row, "data_type", None) or getattr(loc_row, "info_value", loc_row[1])
            if not location or str(location).startswith("view:"):
                return 0.0
            total = self._sum_path_size_bytes(str(location))
            return total / (1024 * 1024) if total else 0.0
        except Exception as e:
            logger.warning(f"Could not get table size for {table_name}: {e}")
            return 0.0

    def _sum_path_size_bytes(self, path: str) -> int:
        """Recursively sum file sizes under path via Hadoop FS."""
        try:
            jvm = self.spark.sparkContext._jvm
            hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
            fs = hadoop_path.getFileSystem(self.spark.sparkContext._jsc.hadoopConfiguration())
            total = 0
            for status in fs.listStatus(hadoop_path) or []:
                if status.isDirectory():
                    total += self._sum_path_size_bytes(status.getPath().toString())
                else:
                    total += status.getLen()
            return int(total)
        except Exception:
            return 0

    def get_raw_input_size_bytes(self, batch_id: int) -> int:
        """Sum file sizes under raw_data_path/Batch{batch_id}/ for throughput metrics."""
        try:
            batch_path = f"{self.raw_data_path}/Batch{batch_id}"
            jvm = self.spark.sparkContext._jvm
            path = jvm.org.apache.hadoop.fs.Path(batch_path)
            fs = path.getFileSystem(self.spark.sparkContext._jsc.hadoopConfiguration())
            total = 0
            for status in fs.listStatus(path) or []:
                if status.isDirectory():
                    for child in fs.listStatus(status.getPath()) or []:
                        if not child.isDirectory():
                            total += child.getLen()
                else:
                    total += status.getLen()
            return int(total)
        except Exception as e:
            logger.warning(f"Could not get raw input size for Batch{batch_id}: {e}")
            return 0
