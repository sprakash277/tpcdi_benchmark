"""
Silver layer loader for Accounts.

Parses and cleans account data.
Implements SCD Type 2 for CDC (Change Data Capture) on incremental loads.

TPC-DI Format Differences:
- Batch 1: CustomerMgmt.xml (XML event log)
- Batch 2+: Account.txt (pipe-delimited state snapshot)
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, to_timestamp, explode, current_timestamp
)
from pyspark.sql.types import LongType, IntegerType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverAccounts(SilverLoaderBase):
    """
    Silver layer loader for Accounts.
    
    Handles two different formats per TPC-DI spec:
    - Batch 1: CustomerMgmt.xml (XML event log)
    - Batch 2+: Account.txt (pipe-delimited state snapshot)
    
    CDC/SCD Type 2 Handling:
    - Batch 1 (Historical): Full load, overwrite from XML
    - Batch 2+ (Incremental): 
      - Parse pipe-delimited Account.txt (state-at-time snapshot)
      - Apply SCD Type 2: Close existing records, insert new versions
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Parse and clean account data.
        
        Args:
            bronze_table: Source bronze table name
              - Batch 1: bronze_customer_mgmt (XML)
              - Batch 2+: bronze_account (pipe-delimited)
            target_table: Target silver table name
            batch_id: Batch number
            
        Returns:
            DataFrame with cleaned account data
        """
        logger.info(f"Loading silver_accounts from {bronze_table} (batch {batch_id})")
        
        if batch_id == 1:
            # Batch 1: Parse XML from CustomerMgmt.xml
            return self._load_from_xml(bronze_table, target_table, batch_id)
        else:
            # Batch 2+: Parse pipe-delimited from Account.txt
            return self._load_from_pipe_delimited(bronze_table, target_table, batch_id)
    
    def _load_from_xml(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load accounts from XML format (Batch 1).
        
        Args:
            bronze_table: bronze_customer_mgmt table
            target_table: Target silver table
            batch_id: Batch number (should be 1)
            
        Returns:
            DataFrame with cleaned account data
        """
        logger.info("Parsing accounts from XML format (Batch 1)")
        
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        account_df = None
        extraction_errors = []
        
        # Pattern 1: Direct access
        try:
            account_df = bronze_df.select(
                col("_ActionType").alias("action_type"),
                col("_ActionTS").alias("action_ts"),
                col("Customer._C_ID").alias("c_id"),
                col("Customer.Account._CA_ID").alias("ca_id"),
                col("Customer.Account._CA_TAX_ST").alias("ca_tax_st"),
                col("Customer.Account.CA_B_ID").alias("ca_b_id"),
                col("Customer.Account.CA_NAME").alias("ca_name"),
                col("_batch_id").alias("batch_id"),
                col("_load_timestamp").alias("load_timestamp"),
            ).filter(col("Customer.Account._CA_ID").isNotNull())
            
            if account_df.count() > 0:
                logger.info("Pattern 1 (direct access) succeeded")
        except Exception as e:
            extraction_errors.append(f"Pattern 1: {e}")
            account_df = None
        
        # Pattern 2: Handle array
        if account_df is None or account_df.count() == 0:
            try:
                for col_name in bronze_df.columns:
                    if "array" in str(bronze_df.schema[col_name].dataType).lower():
                        exploded = bronze_df.select(
                            explode(col(col_name)).alias("Action"),
                            col("_batch_id"), 
                            col("_load_timestamp")
                        )
                        account_df = exploded.select(
                            col("Action._ActionType").alias("action_type"),
                            col("Action._ActionTS").alias("action_ts"),
                            col("Action.Customer._C_ID").alias("c_id"),
                            col("Action.Customer.Account._CA_ID").alias("ca_id"),
                            col("Action.Customer.Account._CA_TAX_ST").alias("ca_tax_st"),
                            col("Action.Customer.Account.CA_B_ID").alias("ca_b_id"),
                            col("Action.Customer.Account.CA_NAME").alias("ca_name"),
                            col("_batch_id").alias("batch_id"),
                            col("_load_timestamp").alias("load_timestamp"),
                        ).filter(col("Action.Customer.Account._CA_ID").isNotNull())
                        
                        if account_df.count() > 0:
                            logger.info("Pattern 2 (explode array) succeeded")
                            break
            except Exception as e:
                extraction_errors.append(f"Pattern 2: {e}")
        
        if account_df is None or account_df.count() == 0:
            raise RuntimeError(f"Failed to extract accounts: {extraction_errors}")
        
        # Transform to silver schema
        silver_df = self._transform_to_silver_schema(
            account_df,
            has_action_type=True,
            batch_id=batch_id
        )
        
        # Batch 1: Full historical load (overwrite)
        return self._write_silver_table(silver_df, target_table, batch_id)
    
    def _load_from_pipe_delimited(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load accounts from pipe-delimited format (Batch 2+).
        
        Account.txt format (pipe-delimited, state-at-time snapshot):
        CA_ID|CA_B_ID|CA_C_ID|CA_NAME|CA_TAX_ST|CA_ST_ID
        
        Args:
            bronze_table: bronze_account table
            target_table: Target silver table
            batch_id: Batch number (should be 2+)
            
        Returns:
            DataFrame with cleaned account data
        """
        logger.info("Parsing accounts from pipe-delimited format (Batch 2+)")
        
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Parse pipe-delimited (approximately 6 columns based on TPC-DI spec)
        parsed_df = self._parse_pipe_delimited(bronze_df, 6)
        
        # Map to account schema
        account_df = parsed_df.select(
            col("_c0").cast(LongType()).alias("ca_id"),
            col("_c1").cast(LongType()).alias("ca_b_id"),
            col("_c2").cast(LongType()).alias("c_id"),
            col("_c3").alias("ca_name"),
            col("_c4").cast(IntegerType()).alias("ca_tax_st"),
            col("_c5").alias("ca_st_id"),  # Status ID
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        # Add action_type and action_ts for state snapshot
        account_df = account_df.withColumn(
            "action_type",
            when(col("ca_st_id") == "INACT", lit("INACT")).otherwise(lit("UPDACCT"))
        ).withColumn(
            "action_ts",
            current_timestamp()
        )
        
        # Transform to silver schema
        silver_df = self._transform_to_silver_schema(
            account_df,
            has_action_type=True,
            batch_id=batch_id
        )
        
        # Batch 2+: Incremental CDC with SCD Type 2
        logger.info(f"Applying SCD Type 2 CDC for accounts batch {batch_id}")
        return self._apply_scd_type2(
            incoming_df=silver_df,
            target_table=target_table,
            key_column="account_id",
            effective_date_col="effective_date"
        )
    
    def _transform_to_silver_schema(self, account_df: DataFrame,
                                    has_action_type: bool, batch_id: int) -> DataFrame:
        """
        Transform account data to silver schema.
        
        Args:
            account_df: DataFrame with account fields
            has_action_type: Whether action_type and action_ts columns exist
            batch_id: Batch number
            
        Returns:
            DataFrame with silver schema
        """
        # Build select list conditionally
        select_cols = [
            col("ca_id").cast(LongType()).alias("sk_account_id"),
            col("ca_id").cast(LongType()).alias("account_id"),
            col("c_id").cast(LongType()).alias("sk_customer_id"),
            col("ca_b_id").cast(LongType()).alias("sk_broker_id"),
        ]
        
        # Status column
        if has_action_type:
            select_cols.append(col("action_type").alias("status"))
        else:
            select_cols.append(lit("ACTIVE").alias("status"))
        
        select_cols.extend([
            col("ca_name").alias("account_desc"),
            col("ca_tax_st").cast(IntegerType()).alias("tax_status"),
        ])
        
        # is_active column
        if has_action_type:
            select_cols.append(
                when(col("action_type") == "INACT", lit(False)).otherwise(lit(True)).alias("is_active")
            )
        else:
            select_cols.append(lit(True).alias("is_active"))
        
        select_cols.append(col("batch_id"))
        
        # is_current column
        if has_action_type:
            select_cols.append(
                when(col("action_type") == "INACT", lit(False)).otherwise(lit(True)).alias("is_current")
            )
        else:
            select_cols.append(lit(True).alias("is_current"))
        
        # effective_date column
        if has_action_type:
            select_cols.append(to_timestamp(col("action_ts")).alias("effective_date"))
        else:
            select_cols.append(current_timestamp().alias("effective_date"))
        
        select_cols.extend([
            lit(None).cast(TimestampType()).alias("end_date"),
            col("load_timestamp"),
        ])
        
        return account_df.select(*select_cols)
