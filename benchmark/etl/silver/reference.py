"""
Silver layer loaders for reference data.

Includes: Date, StatusType, TradeType, Industry
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, to_date
from pyspark.sql.types import IntegerType

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

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
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = silver_df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        logger.info(f"Loaded silver_date: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
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
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = silver_df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        logger.info(f"Loaded silver_status_type: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
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
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = silver_df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        logger.info(f"Loaded silver_trade_type: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
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
        
        # Log timing (detailed only when log_detailed_stats is True)
        start_time = time.time()
        start_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Starting load for {target_table} at {start_datetime}")
        
        self.platform.write_table(silver_df, target_table, mode="overwrite")
        
        end_time = time.time()
        end_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        duration = end_time - start_time
        row_count = silver_df.count()
        
        if table_timing_is_detailed():
            logger.info(f"[TIMING] Completed load for {target_table} at {end_datetime}")
            logger.info(f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite")
        logger.info(f"Loaded silver_industry: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df
