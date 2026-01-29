"""
Base class for Bronze layer ETL loaders.

Provides common functionality for raw data ingestion.
"""

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class BronzeLoaderBase:
    """
    Base class for Bronze layer data loaders.
    
    Provides common functionality:
    - Platform adapter access
    - Metadata column addition
    - Standardized write operations
    """
    
    def __init__(self, platform):
        """
        Initialize Bronze loader.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
    
    def _add_metadata_columns(self, df: DataFrame, source_file: str, batch_id: int) -> DataFrame:
        """
        Add standard metadata columns to raw DataFrame.
        
        Args:
            df: Input DataFrame
            source_file: Name of source file
            batch_id: Batch number
            
        Returns:
            DataFrame with metadata columns added
        """
        return df.withColumn("_load_timestamp", current_timestamp()) \
                 .withColumn("_source_file", lit(source_file)) \
                 .withColumn("_batch_id", lit(batch_id))
    
    def _write_bronze_table(self, df: DataFrame, target_table: str, 
                            batch_id: int, source_file: str) -> DataFrame:
        """
        Write DataFrame to Bronze table with metadata.
        
        Args:
            df: Input DataFrame
            target_table: Full table name (catalog.schema.table)
            batch_id: Batch number
            source_file: Name of source file
            
        Returns:
            DataFrame that was written
        """
        bronze_df = self._add_metadata_columns(df, source_file, batch_id)
        
        # Batch 1 = overwrite, subsequent batches = append
        mode = "overwrite" if batch_id == 1 else "append"
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(bronze_df, target_table, mode=mode)
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = bronze_df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: {mode}")
        table_timing_end(target_table, row_count)
        
        return bronze_df
