"""
Silver layer loader for Trades.

Parses and cleans trade data from bronze_trade.
Implements UPSERT/MERGE for CDC (Change Data Capture) on incremental loads.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, to_timestamp
from pyspark.sql.types import LongType, IntegerType, DoubleType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverTrades(SilverLoaderBase):
    """
    Silver layer loader for Trades.
    
    Parses trade data from bronze_trade pipe-delimited data.
    
    Trade.txt format (14 columns per TPC-DI spec):
    T_ID|T_DTS|T_ST_ID|T_TT_ID|T_IS_CASH|T_S_SYMB|T_QTY|T_BID_PRICE|
    T_CA_ID|T_EXEC_NAME|T_TRADE_PRICE|T_CHRG|T_COMM|T_TAX
    
    CDC Handling:
    - Batch 1 (Historical): Full load, overwrite
    - Batch 2+ (Incremental): UPSERT/MERGE on trade_id
      - Trade records can have status updates (CMPT, CNCL, etc.)
      - New trades are inserted, existing trades are updated
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Parse and clean trade data from bronze_trade.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            batch_id: Batch number
            
        Returns:
            DataFrame with cleaned trade data
        """
        logger.info(f"Loading silver_trades from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Parse pipe-delimited (14 columns per TPC-DI spec)
        parsed_df = self._parse_pipe_delimited(bronze_df, 14)
        
        # Transform to silver schema with proper types
        silver_df = parsed_df.select(
            col("_c0").cast(LongType()).alias("trade_id"),
            to_timestamp(col("_c1")).alias("trade_dts"),
            col("_c2").alias("status_id"),
            col("_c3").alias("trade_type_id"),
            when(col("_c4") == "1", lit(True)).otherwise(lit(False)).alias("is_cash"),
            col("_c5").alias("symbol"),
            col("_c6").cast(IntegerType()).alias("quantity"),
            col("_c7").cast(DoubleType()).alias("bid_price"),
            col("_c8").cast(LongType()).alias("account_id"),
            col("_c9").alias("exec_name"),
            col("_c10").cast(DoubleType()).alias("trade_price"),
            col("_c11").cast(DoubleType()).alias("charge"),
            col("_c12").cast(DoubleType()).alias("commission"),
            col("_c13").cast(DoubleType()).alias("tax"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        # Batch 1: Full historical load (overwrite)
        # Batch 2+: Incremental CDC with UPSERT
        if batch_id == 1:
            return self._write_silver_table(silver_df, target_table, batch_id)
        else:
            # Incremental: Apply UPSERT (MERGE) logic
            # Updates existing trades (status changes) and inserts new trades
            logger.info(f"Applying UPSERT/MERGE CDC for trades batch {batch_id}")
            update_columns = [
                "trade_dts", "status_id", "trade_type_id", "is_cash", "symbol",
                "quantity", "bid_price", "account_id", "exec_name", "trade_price",
                "charge", "commission", "tax", "batch_id", "load_timestamp"
            ]
            return self._upsert_fact_table(
                incoming_df=silver_df,
                target_table=target_table,
                key_column="trade_id",
                update_columns=update_columns
            )
