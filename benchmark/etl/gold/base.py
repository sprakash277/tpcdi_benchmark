"""
Base class for Gold layer ETL loaders.

Gold layer provides business-ready, query-optimized tables:
- Current versions only (no SCD Type 2)
- Denormalized star schema (facts + dimensions)
- Pre-joined tables for analytics
"""

import logging
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

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
        self.platform.write_table(df, target_table, mode=mode)
        logger.info(f"Loaded {target_table}: {df.count()} rows (mode={mode})")
        return df
