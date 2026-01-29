"""
Base class for Gold layer ETL loaders.

Gold layer provides business-ready, query-optimized tables:
- Current versions only (no SCD Type 2)
- Denormalized star schema (facts + dimensions)
- Pre-joined tables for analytics
"""

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class GoldLoaderBase:
    """
    Base class for Gold layer data loaders.
    
    Provides common functionality:
    - Platform adapter access
    - Current version selection from Silver
    - Standardized write operations
    """
    
    def __init__(self, platform):
        """
        Initialize Gold loader.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
    
    def _select_current_version(self, silver_table: str) -> DataFrame:
        """
        Select only current versions from Silver table (is_current = true).
        
        Args:
            silver_table: Silver table name
            
        Returns:
            DataFrame with only current records
        """
        silver_df = self.spark.table(silver_table)
        
        # Filter for current versions only
        if "is_current" in silver_df.columns:
            current_df = silver_df.filter(col("is_current") == True)
            logger.debug(f"Selected {current_df.count()} current records from {silver_table}")
            return current_df
        else:
            # If no is_current column, assume all records are current
            logger.warning(f"No is_current column in {silver_table}, using all records")
            return silver_df
    
    def _write_gold_table(self, df: DataFrame, target_table: str, 
                         mode: str = "overwrite") -> DataFrame:
        """
        Write DataFrame to Gold table.
        
        Args:
            df: Input DataFrame
            target_table: Full table name (catalog.schema.table)
            mode: Write mode (overwrite or append)
            
        Returns:
            DataFrame that was written
        """
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(df, target_table, mode=mode)
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: {mode}")
        table_timing_end(target_table, row_count)
        
        return df
