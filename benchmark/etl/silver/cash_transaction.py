"""
Silver layer loader for Cash Transaction.

Parses cash transaction data from bronze_cash_transaction.
TPC-DI CashTransaction: CT_DTS|T_ID|CT_AMT|CT_NAME (pipe-delimited).
Incremental may have record_type: I|CT_DTS|T_ID|CT_AMT|CT_NAME.
Implements SCD Type 2 for CDC (I/U = insert, D = close only).
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, to_timestamp, coalesce, expr, concat_ws
from pyspark.sql.types import LongType, DoubleType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class SilverCashTransaction(SilverLoaderBase):
    """Silver layer loader for CashTransaction. SCD Type 2 on (t_id, ct_dts) composite."""

    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        logger.info(f"Loading silver_cash_transaction from {bronze_table}")

        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)

        num_cols = 5 if batch_id > 1 else 4
        parsed_df = self._parse_pipe_delimited(bronze_df, num_cols)

        if batch_id > 1:
            record_type_expr = col("_c0").alias("record_type")
            offset = 1
        else:
            record_type_expr = lit("I").alias("record_type")
            offset = 0

        silver_df = parsed_df.select(
            record_type_expr,
            col("_c" + str(offset)).alias("ct_dts"),
            expr("try_cast(trim(_c" + str(offset + 1) + ") AS BIGINT)").alias("t_id"),
            expr("coalesce(try_cast(trim(_c" + str(offset + 2) + ") AS DOUBLE), 0)").alias("ct_amt"),
            coalesce(col("_c" + str(offset + 3)), lit("")).alias("ct_name"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )

        silver_df = silver_df.withColumn(
            "transaction_date",
            to_timestamp(col("ct_dts"))
        ).withColumn(
            "ct_key",
            concat_ws("_", col("t_id").cast("string"), col("ct_dts").cast("string"))
        ).withColumn(
            "effective_date",
            to_timestamp(col("ct_dts"))
        ).withColumn(
            "end_date",
            lit(None).cast(TimestampType())
        ).withColumn(
            "is_current",
            when(col("record_type") == "D", lit(False)).otherwise(lit(True))
        )

        if batch_id == 1:
            self.platform.write_table(silver_df, target_table, mode="overwrite")
            row_count = silver_df.count()
            table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
            return silver_df

        logger.info(f"Applying SCD Type 2 CDC for cash_transaction batch {batch_id} (record_type I/U/D)")
        return self._apply_scd_type2(
            incoming_df=silver_df,
            target_table=target_table,
            key_column="ct_key",
            effective_date_col="effective_date",
            record_type_col="record_type",
            exclude_record_types=["D"],
        )
