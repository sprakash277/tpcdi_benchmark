"""
Silver layer loader for Securities.

Parses security records from bronze_finwire (SEC records).
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, substring
from pyspark.sql.types import LongType, DoubleType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverSecurities(SilverLoaderBase):
    """
    Silver layer loader for Securities.
    
    Parses security records from bronze_finwire fixed-width data.
    
    FINWIRE SEC record layout:
    - Positions 1-15: PTS
    - Positions 16-18: RecType = 'SEC'
    - Positions 19-33: Symbol (15 chars)
    - Positions 34-39: IssueType (6 chars)
    - Positions 40-49: Status (10 chars)
    - Positions 50-119: Name (70 chars)
    - Positions 120-131: ExID (12 chars)
    - Positions 132-149: ShOut (18 chars)
    - Positions 150-165: FirstTradeDate (16 chars)
    - Positions 166-181: FirstTradeExchg (16 chars)
    - Positions 182-189: Dividend (8 chars)
    - Positions 190-249: CoNameOrCIK (60 chars)
    """
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """
        Parse security records from bronze_finwire.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            
        Returns:
            DataFrame with parsed security data
        """
        logger.info(f"Loading silver_securities from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        
        # Filter for SEC records
        sec_df = bronze_df.filter(
            substring(col("raw_line"), 16, 3) == "SEC"
        )
        
        silver_df = sec_df.select(
            trim(substring(col("raw_line"), 1, 15)).alias("pts"),
            trim(substring(col("raw_line"), 19, 15)).alias("symbol"),
            trim(substring(col("raw_line"), 34, 6)).alias("issue_type"),
            trim(substring(col("raw_line"), 40, 10)).alias("status"),
            trim(substring(col("raw_line"), 50, 70)).alias("name"),
            trim(substring(col("raw_line"), 120, 12)).alias("ex_id"),
            trim(substring(col("raw_line"), 132, 18)).cast(LongType()).alias("sh_out"),
            trim(substring(col("raw_line"), 150, 16)).alias("first_trade_date"),
            trim(substring(col("raw_line"), 166, 16)).alias("first_trade_exchg"),
            trim(substring(col("raw_line"), 182, 8)).cast(DoubleType()).alias("dividend"),
            trim(substring(col("raw_line"), 190, 60)).alias("co_name_or_cik"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        
        logger.info(f"Loaded silver_securities: {silver_df.count()} rows")
        return silver_df
