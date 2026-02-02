"""
Silver layer loader for Prospect.

Parses prospect data from bronze_prospect (comma-delimited).
Batch 1: overwrite. Incremental (batch 2+): append only.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, coalesce

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverProspect(SilverLoaderBase):
    """
    Silver layer loader for Prospect.
    Parses comma-delimited Prospect.csv. Batch 1: overwrite. Incremental: append.
    """

    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        logger.info(f"Loading silver_prospect from {bronze_table}")

        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)

        num_cols = 24
        parsed_df = self._parse_csv_delimited(bronze_df, num_cols)

        def c(i: int):
            return coalesce(col(f"_c{i}"), lit(""))

        offset = 1 if batch_id > 1 else 0  # incremental may have record_type in _c0

        silver_df = parsed_df.select(
            c(offset).alias("agency_id"),
            c(offset + 1).alias("last_name"),
            c(offset + 2).alias("first_name"),
            c(offset + 3).alias("middle_initial"),
            c(offset + 4).alias("gender"),
            c(offset + 5).alias("address_line1"),
            c(offset + 6).alias("address_line2"),
            c(offset + 7).alias("city"),
            c(offset + 8).alias("state"),
            c(offset + 9).alias("country"),
            c(offset + 10).alias("phone"),
            c(offset + 11).alias("income"),
            c(offset + 12).alias("number_cars"),
            c(offset + 13).alias("number_children"),
            c(offset + 14).alias("marital_status"),
            c(offset + 15).alias("age"),
            c(offset + 16).alias("credit_rating"),
            c(offset + 17).alias("own_or_rent_flag"),
            c(offset + 18).alias("number_credit_cards"),
            c(offset + 19).alias("company_name"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )

        return self._write_silver_table(silver_df, target_table, batch_id)
