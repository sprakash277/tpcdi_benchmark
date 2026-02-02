"""
Silver layer loader for Daily Market.

Parses and cleans daily market data from bronze_daily_market.
Implements SCD Type 2 for CDC on incremental loads.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date, to_timestamp, concat_ws, coalesce, expr
from pyspark.sql.types import LongType, DoubleType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverDailyMarket(SilverLoaderBase):
    """
    Silver layer loader for Daily Market.
    
    Parses daily market data from bronze_daily_market pipe-delimited data.
    
    DailyMarket.txt format (6 columns):
    DM_DATE|DM_S_SYMB|DM_CLOSE|DM_HIGH|DM_LOW|DM_VOL
    
    CDC Handling:
    - Batch 1 (Historical): Full load, overwrite
    - Batch 2+ (Incremental): SCD Type 2 on dm_key (dm_date + dm_s_symb)
      - I/U: new version with is_current=true, effective_date=dm_date
      - D: close current row only
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Parse and clean daily market data from bronze_daily_market.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            batch_id: Batch number
            
        Returns:
            DataFrame with cleaned daily market data
        """
        logger.info(f"Loading silver_daily_market from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Parse pipe-delimited (7 columns: record_type + 6 data columns)
        # For incremental loads (batch_id > 1), first column is record_type
        # Format: I|DM_DATE|DM_S_SYMB|DM_CLOSE|DM_HIGH|DM_LOW|DM_VOL
        num_cols = 7 if batch_id > 1 else 6  # Batch 1 has no record_type
        parsed_df = self._parse_pipe_delimited(bronze_df, num_cols)
        
        # Check actual column count
        actual_cols = [c for c in parsed_df.columns if c.startswith("_c")]
        max_col_idx = max([int(c[2:]) for c in actual_cols]) if actual_cols else -1
        logger.info(f"DailyMarket.txt parsed with {max_col_idx + 1} columns (indices 0-{max_col_idx})")
        
        # Helper to check if column exists
        def col_exists(idx: int) -> bool:
            return f"_c{idx}" in parsed_df.columns
        
        # Build select list dynamically
        select_cols = []
        
        # For incremental loads (batch_id > 1), capture record_type from _c0
        if batch_id > 1 and col_exists(0):
            select_cols.append(col("_c0").alias("record_type"))
            # Shift all column indices by 1
            col_offset = 1
        else:
            # Batch 1: no record_type, use default
            select_cols.append(lit("I").alias("record_type"))
            col_offset = 0
        
        # Column mapping after record_type:
        # _c{0+offset}: DM_DATE, _c{1+offset}: DM_S_SYMB, etc.
        field_mappings = [
            (0, "dm_date", None, False),  # Will use to_date
            (1, "dm_s_symb", None, False),
            (2, "dm_close", DoubleType, True),
            (3, "dm_high", DoubleType, True),
            (4, "dm_low", DoubleType, True),
            (5, "dm_vol", LongType, True),
        ]
        
        for idx, alias_name, cast_type, is_numeric in field_mappings:
            col_idx = idx + col_offset
            if col_exists(col_idx):
                if alias_name == "dm_date":
                    select_cols.append(to_date(col(f"_c{col_idx}")).alias(alias_name))
                elif cast_type:
                    sql_type = "DOUBLE" if cast_type == DoubleType else "BIGINT"
                    select_cols.append(
                        expr(f"coalesce(try_cast(trim(_c{col_idx}) AS {sql_type}), 0)").alias(alias_name)
                    )
                else:
                    select_cols.append(coalesce(col(f"_c{col_idx}"), lit("")).alias(alias_name))
            else:
                if alias_name == "dm_date":
                    select_cols.append(lit(None).cast("date").alias(alias_name))
                elif cast_type:
                    select_cols.append(lit(0).cast(cast_type()).alias(alias_name))
                else:
                    select_cols.append(lit("").alias(alias_name))
        
        select_cols.extend([
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        ])
        
        silver_df = parsed_df.select(*select_cols)
        
        # Add composite key for potential upsert (date + symbol)
        silver_df = silver_df.withColumn(
            "dm_key",
            concat_ws("_", col("dm_date").cast("string"), col("dm_s_symb"))
        )
        
        # Add SCD Type 2 columns: effective_date, end_date, is_current
        silver_df = silver_df.withColumn(
            "effective_date",
            to_timestamp(col("dm_date").cast("string"))
        ).withColumn(
            "end_date",
            lit(None).cast(TimestampType())
        ).withColumn(
            "is_current",
            when(col("record_type") == "D", lit(False)).otherwise(lit(True))
        )
        
        # Batch 1: Full historical load
        # Batch 2+: Incremental SCD Type 2
        if batch_id == 1:
            return self._write_silver_table(silver_df, target_table, batch_id)
        else:
            logger.info(f"Applying SCD Type 2 CDC for daily_market batch {batch_id} (record_type I/U/D)")
            return self._apply_scd_type2(
                incoming_df=silver_df,
                target_table=target_table,
                key_column="dm_key",
                effective_date_col="effective_date",
                record_type_col="record_type",
                exclude_record_types=["D"],
            )
