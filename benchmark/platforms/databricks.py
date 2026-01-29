"""
Databricks platform adapter for TPC-DI benchmark.
Handles DBFS and Unity Catalog Volume paths.
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
        self.spark = spark
        normalized = raw_data_path.rstrip("/")
        if normalized.startswith("dbfs:/Volumes/"):
            normalized = normalized[5:]  # Remove "dbfs:" prefix
        self.raw_data_path = normalized
        self.use_volume = normalized.startswith("/Volumes/")
        logger.info(f"DatabricksPlatform: raw_data_path='{self.raw_data_path}', use_volume={self.use_volume}")

    def _resolve_path(self, relative_path: str) -> str:
        full = f"{self.raw_data_path}/{relative_path}".replace("//", "/")
        if full.startswith("dbfs:/Volumes/"):
            full = full[5:]
        return full

    def read_raw_file(self, file_path: str, schema: Optional[StructType] = None,
                      format: str = "csv", **options) -> DataFrame:
        full_path = self._resolve_path(file_path)
        reader = self.spark.read.format(format)
        if schema:
            reader = reader.schema(schema)
        for key, value in options.items():
            reader = reader.option(key, value)
        return reader.load(full_path)

    def read_batch_files(self, batch_id: int, file_pattern: str,
                         schema: Optional[StructType] = None, **options) -> DataFrame:
        batch_path = f"Batch{batch_id}/{file_pattern}"
        return self.read_raw_file(batch_path, schema=schema, **options)

    def read_historical_files(self, file_pattern: str,
                              schema: Optional[StructType] = None, **options) -> DataFrame:
        return self.read_batch_files(1, file_pattern, schema=schema, **options)

    def write_table(self, df: DataFrame, table_name: str, mode: str = "overwrite",
                    partition_by: Optional[list] = None, format: str = "delta"):
        logger.info(f"Writing table: {table_name} (mode={mode}, format={format})")
        table_exists = False
        try:
            table_exists = self.spark.catalog.tableExists(table_name)
        except Exception as e:
            logger.warning(f"Could not check if table {table_name} exists: {e}")
        if mode == "overwrite" and format == "delta" and table_exists:
            try:
                self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            except Exception as e:
                logger.warning(f"Could not drop table {table_name}: {e}")
        actual_mode = mode
        if mode == "append" and not table_exists:
            actual_mode = "overwrite"
        writer = df.write.format(format).mode(actual_mode)
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
        When catalog and schema are provided: only check that the catalog exists;
        do not create it. If the catalog does not exist, exit gracefully with an error.
        Then create schema if not exists.
        When catalog/schema not provided: create Hive database.
        """
        exists_clause = "IF NOT EXISTS" if if_not_exists else ""
        if catalog and schema:
            # Only check if catalog exists; do not create it
            try:
                existing = [
                    row.catalog for row in self.spark.sql("SHOW CATALOGS").collect()
                ]
                if catalog not in existing:
                    msg = (
                        f"Catalog '{catalog}' does not exist. "
                        "Please create the catalog (e.g. in Data > Catalogs) and retry."
                    )
                    logger.error(msg)
                    raise ValueError(msg)
            except ValueError:
                raise
            except Exception as e:
                logger.error(f"Could not check catalog existence: {e}")
                raise RuntimeError(
                    f"Could not verify catalog '{catalog}'. "
                    "Please ensure the catalog exists and retry."
                ) from e
            logger.info(f"Catalog '{catalog}' exists; creating or verifying schema.")
            self.spark.sql(f"CREATE SCHEMA {exists_clause} {catalog}.{schema}")
            logger.info(f"Created or verified schema {catalog}.{schema}")
        else:
            self.spark.sql(f"CREATE DATABASE {exists_clause} {database_name}")
            logger.info(f"Created database: {database_name}")

    def get_spark(self) -> SparkSession:
        return self.spark

    def get_table_count(self, table_name: str) -> int:
        result = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").first()
        return result.cnt if result else 0

    def get_table_size_mb(self, table_name: str) -> float:
        try:
            result = self.spark.sql(
                f"SELECT SUM(size) / (1024 * 1024) as size_mb "
                f"FROM (SELECT size FROM DESCRIBE DETAIL {table_name})"
            ).first()
            return result.size_mb if result and result.size_mb else 0.0
        except Exception as e:
            logger.warning(f"Could not get table size: {e}")
            return 0.0
