"""
Base class for Silver layer ETL loaders.

Provides common functionality for data cleaning and transformation.
"""

import logging
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

logger = logging.getLogger(__name__)


class SilverLoaderBase:
    """
    Base class for Silver layer data loaders.
    
    Provides common functionality:
    - Platform adapter access
    - Pipe-delimited parsing
    - CSV parsing
    - Standardized write operations
    """
    
    def __init__(self, platform):
        """
        Initialize Silver loader.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
    
    def _parse_pipe_delimited(self, df: DataFrame, num_cols: int) -> DataFrame:
        """
        Parse pipe-delimited raw_line into columns _c0, _c1, etc.
        
        Args:
            df: DataFrame with 'raw_line' column
            num_cols: Number of expected columns
            
        Returns:
            DataFrame with parsed columns
        """
        temp_view = f"_temp_pipe_parse_{id(df)}"
        df.createOrReplaceTempView(temp_view)
        
        select_parts = []
        for i in range(num_cols):
            select_parts.append(
                f"TRIM(COALESCE(element_at(split(raw_line, '\\\\|'), {i+1}), '')) AS _c{i}"
            )
        
        # Keep metadata columns
        select_parts.extend([
            "_load_timestamp",
            "_source_file", 
            "_batch_id"
        ])
        
        sql = f"SELECT {', '.join(select_parts)} FROM {temp_view}"
        
        try:
            result = self.spark.sql(sql)
        except Exception:
            # Retry without escape if needed
            select_parts = []
            for i in range(num_cols):
                select_parts.append(
                    f"TRIM(COALESCE(element_at(split(raw_line, '|'), {i+1}), '')) AS _c{i}"
                )
            select_parts.extend(["_load_timestamp", "_source_file", "_batch_id"])
            sql = f"SELECT {', '.join(select_parts)} FROM {temp_view}"
            result = self.spark.sql(sql)
        finally:
            try:
                self.spark.catalog.dropTempView(temp_view)
            except:
                pass
        
        return result
    
    def _parse_csv_delimited(self, df: DataFrame, num_cols: int) -> DataFrame:
        """
        Parse comma-delimited raw_line into columns _c0, _c1, etc.
        
        Args:
            df: DataFrame with 'raw_line' column
            num_cols: Number of expected columns
            
        Returns:
            DataFrame with parsed columns
        """
        temp_view = f"_temp_csv_parse_{id(df)}"
        df.createOrReplaceTempView(temp_view)
        
        select_parts = []
        for i in range(num_cols):
            select_parts.append(
                f"TRIM(COALESCE(element_at(split(raw_line, ','), {i+1}), '')) AS _c{i}"
            )
        select_parts.extend(["_load_timestamp", "_source_file", "_batch_id"])
        
        sql = f"SELECT {', '.join(select_parts)} FROM {temp_view}"
        result = self.spark.sql(sql)
        
        try:
            self.spark.catalog.dropTempView(temp_view)
        except:
            pass
        
        return result
    
    def _write_silver_table(self, df: DataFrame, target_table: str, 
                            batch_id: int) -> DataFrame:
        """
        Write DataFrame to Silver table.
        
        Args:
            df: Input DataFrame
            target_table: Full table name (catalog.schema.table)
            batch_id: Batch number
            
        Returns:
            DataFrame that was written
        """
        # Batch 1 = overwrite, subsequent batches = append
        mode = "overwrite" if batch_id == 1 else "append"
        self.platform.write_table(df, target_table, mode=mode)
        
        logger.info(f"Loaded {target_table}: {df.count()} rows (mode={mode})")
        return df
