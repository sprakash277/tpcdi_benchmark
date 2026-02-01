"""
Silver layer loader for Holding History.

Parses holding history from bronze_holding_history (4-column pipe format).
TPC-DI HoldingHistory.txt: HH_H_T_ID|HH_T_ID|HH_BEFORE_QTY|HH_AFTER_QTY
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date, expr
from pyspark.sql.types import LongType

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class SilverHoldingHistory(SilverLoaderBase):
    """
    Silver layer loader for Holding History.

    Parses pipe-delimited HoldingHistory.txt (4 columns):
    HH_H_T_ID|HH_T_ID|HH_BEFORE_QTY|HH_AFTER_QTY

    Joins with silver_trades to enrich with account_id, symbol, trade_dts for gold FactHoldings.
    """

    def load(
        self,
        bronze_table: str,
        target_table: str,
        silver_trades_table: str,
        batch_id: int,
    ) -> DataFrame:
        """
        Parse holding history and enrich with trade data.

        Args:
            bronze_table: bronze_holding_history table name
            target_table: silver_holding_history table name
            silver_trades_table: silver_trades table (for account_id, symbol, trade_dts)
            batch_id: Batch number
        """
        logger.info(f"Loading silver_holding_history from {bronze_table}")

        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)

        # Parse 4-column pipe format: HH_H_T_ID|HH_T_ID|HH_BEFORE_QTY|HH_AFTER_QTY
        num_cols = 4
        parsed_df = self._parse_pipe_delimited(bronze_df, num_cols)

        # Map to schema with try_cast for malformed numeric (e.g. "2939.")
        silver_df = parsed_df.select(
            expr("try_cast(trim(_c0) AS BIGINT)").alias("hh_h_t_id"),
            expr("try_cast(trim(_c1) AS BIGINT)").alias("hh_t_id"),
            expr("coalesce(try_cast(trim(_c2) AS BIGINT), 0)").alias("hh_before_qty"),
            expr("coalesce(try_cast(trim(_c3) AS BIGINT), 0)").alias("hh_after_qty"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )

        # Join with silver_trades to get account_id, symbol, trade_dts
        try:
            trades_df = self.spark.table(silver_trades_table).filter(
                col("batch_id") == batch_id
            ).select(
                col("trade_id"),
                col("account_id"),
                col("symbol"),
                col("trade_dts").alias("holding_date"),
                col("trade_price").alias("purchase_price"),
            )
            silver_df = silver_df.join(
                trades_df,
                silver_df["hh_t_id"] == trades_df["trade_id"],
                "left",
            ).select(
                silver_df["hh_h_t_id"],
                silver_df["hh_t_id"],
                silver_df["hh_before_qty"],
                silver_df["hh_after_qty"],
                col("account_id"),
                col("symbol"),
                col("holding_date"),
                col("hh_after_qty").alias("quantity"),
                col("purchase_price"),
                to_date(col("holding_date")).alias("purchase_date"),
                silver_df["batch_id"],
                silver_df["load_timestamp"],
            )
        except Exception as e:
            logger.warning(f"Could not join with silver_trades, outputting base holding columns only: {e}")
            silver_df = silver_df.select(
                col("hh_h_t_id"),
                col("hh_t_id"),
                col("hh_before_qty"),
                col("hh_after_qty"),
                lit(None).cast(LongType()).alias("account_id"),
                lit(None).cast("string").alias("symbol"),
                lit(None).cast("timestamp").alias("holding_date"),
                col("hh_after_qty").alias("quantity"),
                lit(None).cast("double").alias("purchase_price"),
                lit(None).cast("date").alias("purchase_date"),
                col("batch_id"),
                col("load_timestamp"),
            )

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
            logger.info(
                f"[TIMING] {target_table} - Start: {start_datetime}, End: {end_datetime}, "
                f"Duration: {duration:.2f}s, Rows: {row_count}, Mode: overwrite"
            )
        logger.info(f"Loaded silver_holding_history: {row_count} rows")
        table_timing_end(
            target_table,
            row_count,
            bytes_processed=_get_table_size_bytes(self.platform, target_table),
        )
        return silver_df
