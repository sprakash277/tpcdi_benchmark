"""
Incremental ETL transformations for TPC-DI benchmark.
Handles incremental batch processing.
"""

import logging
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, lit, current_timestamp

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

logger = logging.getLogger(__name__)


class IncrementalETL:
    """Incremental ETL processor for TPC-DI."""
    
    def __init__(self, platform):
        """
        Initialize incremental ETL processor.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
        logger.info("Initialized IncrementalETL processor")
    
    def process_batch(self, batch_id: int, target_database: str, target_schema: str):
        """
        Process a single incremental batch.
        
        Args:
            batch_id: Batch number to process
            target_database: Target database name
            target_schema: Target schema name
        """
        logger.info(f"Processing incremental batch {batch_id}")
        
        # Load new/updated accounts
        self.load_dim_account_incremental(
            batch_id,
            f"{target_database}.{target_schema}.DimAccount"
        )
        
        # Load new trades
        self.load_fact_trade_incremental(
            batch_id,
            f"{target_database}.{target_schema}.FactTrade"
        )
        
        # Load new/updated customers
        self.load_dim_customer_incremental(
            batch_id,
            f"{target_database}.{target_schema}.DimCustomer"
        )
        
        logger.info(f"Completed processing batch {batch_id}")
    
    def load_dim_account_incremental(self, batch_id: int, target_table: str) -> DataFrame:
        """Load Account dimension updates from batch files"""
        logger.info(f"Loading DimAccount incremental updates from batch {batch_id}")
        
        # Read CustomerMgmt.txt for this batch
        df = self.platform.read_batch_files(
            batch_id,
            "CustomerMgmt.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        # Filter for Account records
        account_records = df.filter(col("_c0") == "A")
        
        # Transform to DimAccount schema
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
        
        # For incremental: merge with existing data (SCD Type 2)
        # This is simplified - actual implementation would handle SCD properly
        self.platform.write_table(dim_account, target_table, mode="append")
        return dim_account
    
    def load_fact_trade_incremental(self, batch_id: int, target_table: str) -> DataFrame:
        """Load Trade fact table from batch files"""
        logger.info(f"Loading FactTrade from batch {batch_id}")
        
        # Read Trade.txt for this batch
        df = self.platform.read_batch_files(
            batch_id,
            "Trade.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        # Transform to FactTrade schema (simplified)
        fact_trade = df.select(
            col("_c0").alias("TradeID"),
            col("_c1").alias("SK_BrokerID"),
            col("_c2").alias("SK_CreateDateID"),
            col("_c3").alias("SK_CreateTimeID"),
            col("_c4").alias("SK_CloseDateID"),
            col("_c5").alias("SK_CloseTimeID"),
            col("_c6").alias("Status"),
            col("_c7").alias("Type"),
            col("_c8").alias("CashFlag"),
            col("_c9").alias("Quantity"),
            col("_c10").alias("BidPrice"),
            col("_c11").alias("Commission"),
            col("_c12").alias("Charge"),
            lit(batch_id).alias("BatchID")
        )
        
        self.platform.write_table(fact_trade, target_table, mode="append")
        return fact_trade
    
    def load_dim_customer_incremental(self, batch_id: int, target_table: str) -> DataFrame:
        """Load Customer dimension updates from batch files"""
        logger.info(f"Loading DimCustomer incremental updates from batch {batch_id}")
        
        # Read CustomerMgmt.txt for this batch
        df = self.platform.read_batch_files(
            batch_id,
            "CustomerMgmt.txt",
            format="csv",
            sep="|",
            header=False,
            inferSchema=True
        )
        
        # Filter for Customer records (record type 'C')
        customer_records = df.filter(col("_c0") == "C")
        
        # Transform to DimCustomer schema (simplified)
        dim_customer = customer_records.select(
            col("_c1").alias("CustomerID"),
            col("_c2").alias("TaxID"),
            col("_c3").alias("Status"),
            col("_c4").alias("LastName"),
            col("_c5").alias("FirstName"),
            col("_c6").alias("MiddleInitial"),
            col("_c7").alias("Gender"),
            col("_c8").alias("Tier"),
            col("_c9").alias("DOB"),
            col("_c10").alias("AddressLine1"),
            col("_c11").alias("AddressLine2"),
            col("_c12").alias("PostalCode"),
            col("_c13").alias("City"),
            col("_c14").alias("StateProv"),
            col("_c15").alias("Country"),
            col("_c16").alias("Phone1"),
            col("_c17").alias("Phone2"),
            col("_c18").alias("Phone3"),
            col("_c19").alias("Email1"),
            col("_c20").alias("Email2"),
            col("_c21").alias("NationalTaxRateDesc"),
            col("_c22").alias("NationalTaxRate"),
            col("_c23").alias("LocalTaxRateDesc"),
            col("_c24").alias("LocalTaxRate"),
            lit(batch_id).alias("BatchID"),
            current_timestamp().alias("EffectiveDate"),
            current_timestamp().alias("EndDate")
        )
        
        # For incremental: merge with existing data (SCD Type 2)
        self.platform.write_table(dim_customer, target_table, mode="append")
        return dim_customer
