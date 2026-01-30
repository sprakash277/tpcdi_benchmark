"""
Silver layer loader for Companies.

Parses company records from bronze_finwire (CMP records).
"""

import logging
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, substring
from pyspark.sql.types import LongType

from benchmark.etl.silver.base import SilverLoaderBase, _get_table_size_bytes
from benchmark.etl.table_timing import end_table as table_timing_end, is_detailed as table_timing_is_detailed

logger = logging.getLogger(__name__)


class SilverCompanies(SilverLoaderBase):
    """
    Silver layer loader for Companies.
    
    Parses company records from bronze_finwire fixed-width data.
    
    FINWIRE CMP record layout (per TPC-DI spec):
    - Positions 1-15: PTS (timestamp)
    - Positions 16-18: RecType = 'CMP'
    - Positions 19-78: CompanyName (60 chars)
    - Positions 79-88: CIK (10 chars)
    - Positions 89-92: Status (4 chars)
    - Positions 93-102: IndustryID (10 chars)
    - Positions 103-106: SPRating (4 chars)
    - Positions 107-114: FoundingDate (8 chars, YYYYMMDD)
    - Positions 115-129: CEOName (15 chars)
    - Positions 130-174: AddressLine1 (45 chars)
    - Positions 175-219: AddressLine2 (45 chars)
    - Positions 220-244: PostalCode (25 chars)
    - Positions 245-269: City (25 chars)
    - Positions 270-294: StateProvince (25 chars)
    - Positions 295-319: Country (25 chars)
    - Positions 320-364: Description (45 chars)
    """
    
    def load(self, bronze_table: str, target_table: str) -> DataFrame:
        """
        Parse company records from bronze_finwire.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            
        Returns:
            DataFrame with parsed company data
        """
        logger.info(f"Loading silver_companies from {bronze_table}")
        
        bronze_df = self.spark.table(bronze_table)
        
        # Filter for CMP records (RecType at positions 16-18)
        cmp_df = bronze_df.filter(
            substring(col("raw_line"), 16, 3) == "CMP"
        )
        
        # Parse fixed-width fields
        silver_df = cmp_df.select(
            trim(substring(col("raw_line"), 1, 15)).alias("pts"),
            trim(substring(col("raw_line"), 19, 60)).alias("company_name"),
            trim(substring(col("raw_line"), 79, 10)).alias("cik"),
            trim(substring(col("raw_line"), 89, 4)).alias("status"),
            trim(substring(col("raw_line"), 93, 10)).alias("industry_id"),
            trim(substring(col("raw_line"), 103, 4)).alias("sp_rating"),
            trim(substring(col("raw_line"), 107, 8)).alias("founding_date"),
            trim(substring(col("raw_line"), 115, 15)).alias("ceo_name"),
            trim(substring(col("raw_line"), 130, 45)).alias("address_line1"),
            trim(substring(col("raw_line"), 175, 45)).alias("address_line2"),
            trim(substring(col("raw_line"), 220, 25)).alias("postal_code"),
            trim(substring(col("raw_line"), 245, 25)).alias("city"),
            trim(substring(col("raw_line"), 270, 25)).alias("state_province"),
            trim(substring(col("raw_line"), 295, 25)).alias("country"),
            trim(substring(col("raw_line"), 320, 45)).alias("description"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        # Add surrogate key based on CIK
        silver_df = silver_df.withColumn(
            "sk_company_id", 
            col("cik").cast(LongType())
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
        logger.info(f"Loaded silver_companies: {row_count} rows")
        table_timing_end(target_table, row_count, bytes_processed=_get_table_size_bytes(self.platform, target_table))
        return silver_df
