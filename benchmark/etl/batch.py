"""
Batch ETL transformations for TPC-DI benchmark.
Handles historical load and initial batch processing.
"""

import logging
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, lit, current_timestamp

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

logger = logging.getLogger(__name__)


class BatchETL:
    """Batch ETL processor for TPC-DI."""
    
    def __init__(self, platform):
        """
        Initialize batch ETL processor.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
        logger.info("Initialized BatchETL processor")
    
    def load_dim_date(self, target_table: str) -> DataFrame:
        """Load Date dimension table from HistoricalLoad/Date.txt"""
        logger.info("Loading DimDate dimension table")
        
        # Read Date.txt - format: DateValue|DayOfWeek|CalendarMonth|CalendarQuarter|CalendarYear|DayOfMonth
        df = self.platform.read_historical_files(
            "Date.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        # Transform to DimDate schema
        dim_date = df.select(
            col("_c0").alias("SK_DateID"),
            col("_c0").alias("DateValue"),
            col("_c1").alias("DateDesc"),
            col("_c2").alias("CalendarMonthID"),
            col("_c2").alias("CalendarMonthDesc"),
            col("_c3").alias("CalendarQuarterID"),
            col("_c3").alias("CalendarQuarterDesc"),
            col("_c4").alias("CalendarYear"),
            col("_c5").alias("DayOfWeek"),
            col("_c5").alias("DayOfWeekDesc"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_date, target_table, mode="overwrite")
        return dim_date
    
    def load_dim_time(self, target_table: str) -> DataFrame:
        """Load Time dimension table from HistoricalLoad/Time.txt"""
        logger.info("Loading DimTime dimension table")
        
        # Read Time.txt - format: Time|TimeID|Hour|Minute|Second|AM|PM
        df = self.platform.read_historical_files(
            "Time.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        # Transform to DimTime schema
        dim_time = df.select(
            col("_c1").alias("SK_TimeID"),
            col("_c0").alias("TimeValue"),
            col("_c2").alias("HourID"),
            col("_c2").alias("HourDesc"),
            col("_c3").alias("MinuteID"),
            col("_c3").alias("MinuteDesc"),
            col("_c4").alias("SecondID"),
            col("_c4").alias("SecondDesc"),
            col("_c5").alias("MarketHoursFlag"),
            col("_c6").alias("OfficeHoursFlag"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_time, target_table, mode="overwrite")
        return dim_time
    
    def load_dim_trade_type(self, target_table: str) -> DataFrame:
        """Load TradeType dimension from HistoricalLoad/TradeType.txt"""
        logger.info("Loading DimTradeType dimension table")
        
        df = self.platform.read_historical_files(
            "TradeType.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        dim_trade_type = df.select(
            col("_c0").alias("TT_ID"),
            col("_c1").alias("TT_NAME"),
            col("_c2").alias("TT_IS_SELL"),
            col("_c3").alias("TT_IS_MRKT"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_trade_type, target_table, mode="overwrite")
        return dim_trade_type
    
    def load_dim_status_type(self, target_table: str) -> DataFrame:
        """Load StatusType dimension from HistoricalLoad/StatusType.txt"""
        logger.info("Loading DimStatusType dimension table")
        
        df = self.platform.read_historical_files(
            "StatusType.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        dim_status_type = df.select(
            col("_c0").alias("ST_ID"),
            col("_c1").alias("ST_NAME"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_status_type, target_table, mode="overwrite")
        return dim_status_type
    
    def load_dim_tax_rate(self, target_table: str) -> DataFrame:
        """Load TaxRate dimension from HistoricalLoad/TaxRate.txt"""
        logger.info("Loading DimTaxRate dimension table")
        
        df = self.platform.read_historical_files(
            "TaxRate.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        dim_tax_rate = df.select(
            col("_c0").alias("TX_ID"),
            col("_c1").alias("TX_NAME"),
            col("_c2").alias("TX_RATE"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_tax_rate, target_table, mode="overwrite")
        return dim_tax_rate
    
    def load_dim_industry(self, target_table: str) -> DataFrame:
        """Load Industry dimension from HistoricalLoad/Industry.txt"""
        logger.info("Loading DimIndustry dimension table")
        
        df = self.platform.read_historical_files(
            "Industry.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        dim_industry = df.select(
            col("_c0").alias("IN_ID"),
            col("_c1").alias("IN_NAME"),
            col("_c2").alias("IN_SC_ID"),
            col("_c3").alias("IN_SC_NAME"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_industry, target_table, mode="overwrite")
        return dim_industry
    
    def load_dim_account(self, target_table: str, batch_id: int = 1) -> DataFrame:
        """Load Account dimension from Batch files (CustomerMgmt.txt)"""
        logger.info(f"Loading DimAccount dimension table from batch {batch_id}")
        
        # Read CustomerMgmt.txt - this is a complex file with multiple record types
        df = self.platform.read_batch_files(
            batch_id,
            "CustomerMgmt.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        # Filter for Account records (record type 'A')
        account_records = df.filter(col("_c0") == "A")
        
        # Transform to DimAccount schema (simplified - actual TPC-DI spec is more complex)
        dim_account = account_records.select(
            col("_c1").alias("AccountID"),
            col("_c2").alias("SK_BrokerID"),
            col("_c3").alias("SK_CustomerID"),
            col("_c4").alias("Status"),
            col("_c5").alias("AccountDesc"),
            col("_c6").alias("TaxStatus"),
            col("_c7").alias("IsCurrent"),
            lit(batch_id).alias("BatchID"),
            current_timestamp().alias("EffectiveDate"),
            current_timestamp().alias("EndDate")
        )
        
        self.platform.write_table(dim_account, target_table, mode="append")
        return dim_account
    
    def run_full_batch_load(self, target_database: str, target_schema: str):
        """
        Run complete batch load process.
        
        Args:
            target_database: Target database name
            target_schema: Target schema name
        """
        logger.info("Starting full batch load process")
        
        # Create database and schema
        self.platform.create_database(target_database)
        
        # Load dimension tables (historical)
        self.load_dim_date(f"{target_database}.{target_schema}.DimDate")
        self.load_dim_time(f"{target_database}.{target_schema}.DimTime")
        self.load_dim_trade_type(f"{target_database}.{target_schema}.DimTradeType")
        self.load_dim_status_type(f"{target_database}.{target_schema}.DimStatusType")
        self.load_dim_tax_rate(f"{target_database}.{target_schema}.DimTaxRate")
        self.load_dim_industry(f"{target_database}.{target_schema}.DimIndustry")
        
        # Load fact and other dimension tables from Batch1
        self.load_dim_account(f"{target_database}.{target_schema}.DimAccount", batch_id=1)
        
        logger.info("Completed full batch load process")
