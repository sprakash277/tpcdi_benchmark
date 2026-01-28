"""
Silver layer loader for Financials.

Parses financial records from bronze_finwire (FIN records).
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, substring
from pyspark.sql.types import IntegerType, LongType, DoubleType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverFinancials(SilverLoaderBase):
    """
    Silver layer loader for Financials.
    
    Parses financial records from bronze_finwire fixed-width data.
    
    FINWIRE FIN record layout:
    - Positions 1-15: PTS
    - Positions 16-18: RecType = 'FIN'
    - Positions 19-22: Year (4 chars)
    - Positions 23: Quarter (1 char)
    - Positions 24-33: QtrStartDate (10 chars)
    - Positions 34-50: PostingDate (17 chars)
    - Positions 51-67: Revenue (17 chars)
    - Positions 68-84: Earnings (17 chars)
    - Positions 85-101: EPS (17 chars)
    - Positions 102-118: DilutedEPS (17 chars)
    - Positions 119-135: Margin (17 chars)
    - Positions 136-152: Inventory (17 chars)
    - Positions 153-169: Assets (17 chars)
    - Positions 170-186: Liabilities (17 chars)
    - Positions 187-203: ShOut (17 chars)
    - Positions 204-213: DilutedShOut (10 chars)
    - Positions 214-273: CoNameOrCIK (60 chars)
    """
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """
        Parse financial records from bronze_finwire.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            
        Returns:
            DataFrame with parsed financial data
        """
        logger.info(f"Loading silver_financials from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        
        # Filter for FIN records
        fin_df = bronze_df.filter(
            substring(col("raw_line"), 16, 3) == "FIN"
        )
        
        silver_df = fin_df.select(
            trim(substring(col("raw_line"), 1, 15)).alias("pts"),
            trim(substring(col("raw_line"), 19, 4)).cast(IntegerType()).alias("year"),
            trim(substring(col("raw_line"), 23, 1)).cast(IntegerType()).alias("quarter"),
            trim(substring(col("raw_line"), 24, 10)).alias("qtr_start_date"),
            trim(substring(col("raw_line"), 34, 17)).alias("posting_date"),
            trim(substring(col("raw_line"), 51, 17)).cast(DoubleType()).alias("revenue"),
            trim(substring(col("raw_line"), 68, 17)).cast(DoubleType()).alias("earnings"),
            trim(substring(col("raw_line"), 85, 17)).cast(DoubleType()).alias("eps"),
            trim(substring(col("raw_line"), 102, 17)).cast(DoubleType()).alias("diluted_eps"),
            trim(substring(col("raw_line"), 119, 17)).cast(DoubleType()).alias("margin"),
            trim(substring(col("raw_line"), 136, 17)).cast(DoubleType()).alias("inventory"),
            trim(substring(col("raw_line"), 153, 17)).cast(DoubleType()).alias("assets"),
            trim(substring(col("raw_line"), 170, 17)).cast(DoubleType()).alias("liabilities"),
            trim(substring(col("raw_line"), 187, 17)).cast(LongType()).alias("sh_out"),
            trim(substring(col("raw_line"), 204, 10)).cast(LongType()).alias("diluted_sh_out"),
            trim(substring(col("raw_line"), 214, 60)).alias("co_name_or_cik"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        
        logger.info(f"Loaded silver_financials: {silver_df.count()} rows")
        return silver_df
