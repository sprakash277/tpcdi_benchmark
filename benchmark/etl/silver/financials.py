"""
Silver layer loader for Financials.

Parses financial records from bronze_finwire (FIN records).
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, substring, expr

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

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
            expr("try_cast(trim(substring(raw_line, 19, 4)) AS INT)").alias("year"),
            expr("try_cast(trim(substring(raw_line, 23, 1)) AS INT)").alias("quarter"),
            trim(substring(col("raw_line"), 24, 10)).alias("qtr_start_date"),
            trim(substring(col("raw_line"), 34, 17)).alias("posting_date"),
            expr("try_cast(trim(substring(raw_line, 51, 17)) AS DOUBLE)").alias("revenue"),
            expr("try_cast(trim(substring(raw_line, 68, 17)) AS DOUBLE)").alias("earnings"),
            expr("try_cast(trim(substring(raw_line, 85, 17)) AS DOUBLE)").alias("eps"),
            expr("try_cast(trim(substring(raw_line, 102, 17)) AS DOUBLE)").alias("diluted_eps"),
            expr("try_cast(trim(substring(raw_line, 119, 17)) AS DOUBLE)").alias("margin"),
            expr("try_cast(trim(substring(raw_line, 136, 17)) AS DOUBLE)").alias("inventory"),
            expr("try_cast(trim(substring(raw_line, 153, 17)) AS DOUBLE)").alias("assets"),
            expr("try_cast(trim(substring(raw_line, 170, 17)) AS DOUBLE)").alias("liabilities"),
            expr("try_cast(trim(substring(raw_line, 187, 17)) AS BIGINT)").alias("sh_out"),
            expr("try_cast(trim(substring(raw_line, 204, 10)) AS BIGINT)").alias("diluted_sh_out"),
            trim(substring(col("raw_line"), 214, 60)).alias("co_name_or_cik"),
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
        logger.info(f"Loaded silver_financials: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df
