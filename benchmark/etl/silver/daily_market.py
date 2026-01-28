"""
Silver layer loader for Daily Market.

Parses and cleans daily market data from bronze_daily_market.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import LongType, DoubleType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverDailyMarket(SilverLoaderBase):
    """
    Silver layer loader for Daily Market.
    
    Parses daily market data from bronze_daily_market pipe-delimited data.
    
    DailyMarket.txt format (6 columns):
    DM_DATE|DM_S_SYMB|DM_CLOSE|DM_HIGH|DM_LOW|DM_VOL
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Parse and clean daily market data from bronze_daily_market.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            batch_id: Batch number
            
        Returns:
            DataFrame with cleaned daily market data
        """
        logger.info(f"Loading silver_daily_market from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Parse pipe-delimited (6 columns)
        parsed_df = self._parse_pipe_delimited(bronze_df, 6)
        
        silver_df = parsed_df.select(
            to_date(col("_c0")).alias("dm_date"),
            col("_c1").alias("dm_s_symb"),
            col("_c2").cast(DoubleType()).alias("dm_close"),
            col("_c3").cast(DoubleType()).alias("dm_high"),
            col("_c4").cast(DoubleType()).alias("dm_low"),
            col("_c5").cast(LongType()).alias("dm_vol"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        return self._write_silver_table(silver_df, target_table, batch_id)
