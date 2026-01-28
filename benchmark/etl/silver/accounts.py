"""
Silver layer loader for Accounts.

Parses and cleans account data from bronze_customer_mgmt.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, to_timestamp, explode
)
from pyspark.sql.types import LongType, IntegerType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverAccounts(SilverLoaderBase):
    """
    Silver layer loader for Accounts.
    
    Extracts account attributes from bronze_customer_mgmt XML structure:
    - Customer.Account._CA_ID, _CA_TAX_ST
    - Customer.Account.CA_B_ID (broker), CA_NAME
    
    Handles SCD Type 2 versioning based on ActionType.
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Parse and clean account data from bronze_customer_mgmt.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            batch_id: Batch number
            
        Returns:
            DataFrame with cleaned account data
        """
        logger.info(f"Loading silver_accounts from {bronze_table}")
        
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
        silver_df = account_df.select(
            col("ca_id").cast(LongType()).alias("sk_account_id"),
            col("ca_id").cast(LongType()).alias("account_id"),
            col("c_id").cast(LongType()).alias("sk_customer_id"),
            col("ca_b_id").cast(LongType()).alias("sk_broker_id"),
            col("action_type").alias("status"),
            col("ca_name").alias("account_desc"),
            col("ca_tax_st").cast(IntegerType()).alias("tax_status"),
            when(col("action_type") == "INACT", lit(False)).otherwise(lit(True)).alias("is_active"),
            col("batch_id"),
            when(col("action_type") == "INACT", lit(False)).otherwise(lit(True)).alias("is_current"),
            to_timestamp(col("action_ts")).alias("effective_date"),
            lit(None).cast(TimestampType()).alias("end_date"),
            col("load_timestamp"),
        )
        
        return self._write_silver_table(silver_df, target_table, batch_id)
