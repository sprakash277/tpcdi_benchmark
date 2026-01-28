"""
Silver layer loaders for reference data.

Includes: Date, StatusType, TradeType, Industry
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, to_date
from pyspark.sql.types import IntegerType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverDate(SilverLoaderBase):
    """Silver layer loader for Date dimension."""
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse and clean Date dimension from bronze_date."""
        logger.info(f"Loading silver_date from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 18)
        
        silver_df = parsed_df.select(
            col("_c0").cast(IntegerType()).alias("sk_date_id"),
            to_date(col("_c1")).alias("date_value"),
            col("_c2").alias("date_desc"),
            col("_c3").cast(IntegerType()).alias("calendar_year_id"),
            col("_c4").alias("calendar_year_desc"),
            col("_c5").cast(IntegerType()).alias("calendar_qtr_id"),
            col("_c6").alias("calendar_qtr_desc"),
            col("_c7").cast(IntegerType()).alias("calendar_month_id"),
            col("_c8").alias("calendar_month_desc"),
            col("_c9").cast(IntegerType()).alias("calendar_week_id"),
            col("_c10").alias("calendar_week_desc"),
            col("_c11").cast(IntegerType()).alias("day_of_week_num"),
            col("_c12").alias("day_of_week_desc"),
            col("_c13").cast(IntegerType()).alias("fiscal_year_id"),
            col("_c14").alias("fiscal_year_desc"),
            col("_c15").cast(IntegerType()).alias("fiscal_qtr_id"),
            col("_c16").alias("fiscal_qtr_desc"),
            when(col("_c17") == "true", lit(True)).otherwise(lit(False)).alias("holiday_flag"),
            col("_batch_id").alias("batch_id"),
        )
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        logger.info(f"Loaded silver_date: {silver_df.count()} rows")
        return silver_df


class SilverStatusType(SilverLoaderBase):
    """Silver layer loader for StatusType."""
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse StatusType.txt (ST_ID|ST_NAME)."""
        logger.info(f"Loading silver_status_type from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 2)
        
        silver_df = parsed_df.select(
            col("_c0").alias("st_id"),
            col("_c1").alias("st_name"),
        )
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        logger.info(f"Loaded silver_status_type: {silver_df.count()} rows")
        return silver_df


class SilverTradeType(SilverLoaderBase):
    """Silver layer loader for TradeType."""
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse TradeType.txt (TT_ID|TT_NAME|TT_IS_SELL|TT_IS_MRKT)."""
        logger.info(f"Loading silver_trade_type from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 4)
        
        silver_df = parsed_df.select(
            col("_c0").alias("tt_id"),
            col("_c1").alias("tt_name"),
            when(col("_c2") == "1", lit(True)).otherwise(lit(False)).alias("tt_is_sell"),
            when(col("_c3") == "1", lit(True)).otherwise(lit(False)).alias("tt_is_mrkt"),
        )
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        logger.info(f"Loaded silver_trade_type: {silver_df.count()} rows")
        return silver_df


class SilverIndustry(SilverLoaderBase):
    """Silver layer loader for Industry."""
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """Parse Industry.txt (IN_ID|IN_NAME|IN_SC_ID|IN_SC_NAME)."""
        logger.info(f"Loading silver_industry from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        parsed_df = self._parse_pipe_delimited(bronze_df, 4)
        
        silver_df = parsed_df.select(
            col("_c0").alias("in_id"),
            col("_c1").alias("in_name"),
            col("_c2").alias("in_sc_id"),
            col("_c3").alias("in_sc_name"),
        )
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        logger.info(f"Loaded silver_industry: {silver_df.count()} rows")
        return silver_df
