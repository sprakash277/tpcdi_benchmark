"""
Silver layer loader for Securities.

Parses security records from bronze_finwire (SEC records).
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, substring, expr

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

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
            expr("try_cast(trim(substring(raw_line, 132, 18)) AS BIGINT)").alias("sh_out"),
            trim(substring(col("raw_line"), 150, 16)).alias("first_trade_date"),
            trim(substring(col("raw_line"), 166, 16)).alias("first_trade_exchg"),
            expr("try_cast(trim(substring(raw_line, 182, 8)) AS DOUBLE)").alias("dividend"),
            trim(substring(col("raw_line"), 190, 60)).alias("co_name_or_cik"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
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
        logger.info(f"Loaded silver_securities: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df
