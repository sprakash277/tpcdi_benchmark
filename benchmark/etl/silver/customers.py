"""
Silver layer loader for Customers.

Parses and cleans customer data from bronze_customer_mgmt.
Implements SCD Type 2 for CDC (Change Data Capture) on incremental loads.
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, to_date, to_timestamp, explode
)
from pyspark.sql.types import LongType, IntegerType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverCustomers(SilverLoaderBase):
    """
    Silver layer loader for Customers.
    
    Extracts customer attributes from bronze_customer_mgmt XML structure:
    - Customer._C_ID, _C_TAX_ID, _C_GNDR, _C_TIER, _C_DOB
    - Customer.Name.C_L_NAME, C_F_NAME, C_M_NAME
    - Customer.Address.*, ContactInfo.*, TaxInfo.*
    
    CDC/SCD Type 2 Handling:
    - Batch 1 (Historical): Full load, overwrite
    - Batch 2+ (Incremental): 
      - NEW: Insert new customer
      - UPDCUST: Close existing record, insert new version
      - INACT: Close existing record, insert inactive version
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Parse and clean customer data from bronze_customer_mgmt.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            batch_id: Batch number
            
        Returns:
            DataFrame with cleaned customer data
        """
        logger.info(f"Loading silver_customers from {bronze_table}")
        
        # Read bronze table (contains nested XML structure)
        bronze_df = self.spark.table(bronze_table)
        
        # Filter for current batch
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Extract customer fields from XML structure
        customer_df = None
        extraction_errors = []
        
        # Pattern 1: Direct access (TPCDI:Action as row)
        try:
            customer_df = bronze_df.select(
                col("_ActionType").alias("action_type"),
                col("_ActionTS").alias("action_ts"),
                col("Customer._C_ID").alias("c_id"),
                col("Customer._C_TAX_ID").alias("c_tax_id"),
                col("Customer._C_GNDR").alias("c_gndr"),
                col("Customer._C_TIER").alias("c_tier"),
                col("Customer._C_DOB").alias("c_dob"),
                col("Customer.Name.C_L_NAME").alias("c_l_name"),
                col("Customer.Name.C_F_NAME").alias("c_f_name"),
                col("Customer.Name.C_M_NAME").alias("c_m_name"),
                col("Customer.Address.C_ADLINE1").alias("c_adline1"),
                col("Customer.Address.C_ADLINE2").alias("c_adline2"),
                col("Customer.Address.C_ZIPCODE").alias("c_zipcode"),
                col("Customer.Address.C_CITY").alias("c_city"),
                col("Customer.Address.C_STATE_PROV").alias("c_state_prov"),
                col("Customer.Address.C_CTRY").alias("c_ctry"),
                col("Customer.ContactInfo.C_PRIM_EMAIL").alias("c_prim_email"),
                col("Customer.ContactInfo.C_ALT_EMAIL").alias("c_alt_email"),
                col("Customer.TaxInfo.C_LCL_TX_ID").alias("c_lcl_tx_id"),
                col("Customer.TaxInfo.C_NAT_TX_ID").alias("c_nat_tx_id"),
                col("_batch_id").alias("batch_id"),
                col("_load_timestamp").alias("load_timestamp"),
            ).filter(col("Customer._C_ID").isNotNull())
            
            if customer_df.count() > 0:
                logger.info("Pattern 1 (direct access) succeeded")
        except Exception as e:
            extraction_errors.append(f"Pattern 1: {e}")
            customer_df = None
        
        # Pattern 2: Handle if Actions are in array
        if customer_df is None or customer_df.count() == 0:
            try:
                for col_name in bronze_df.columns:
                    if "array" in str(bronze_df.schema[col_name].dataType).lower():
                        exploded = bronze_df.select(
                            explode(col(col_name)).alias("Action"), 
                            col("_batch_id"), 
                            col("_load_timestamp")
                        )
                        customer_df = exploded.select(
                            col("Action._ActionType").alias("action_type"),
                            col("Action._ActionTS").alias("action_ts"),
                            col("Action.Customer._C_ID").alias("c_id"),
                            col("Action.Customer._C_TAX_ID").alias("c_tax_id"),
                            col("Action.Customer._C_GNDR").alias("c_gndr"),
                            col("Action.Customer._C_TIER").alias("c_tier"),
                            col("Action.Customer._C_DOB").alias("c_dob"),
                            col("Action.Customer.Name.C_L_NAME").alias("c_l_name"),
                            col("Action.Customer.Name.C_F_NAME").alias("c_f_name"),
                            col("Action.Customer.Name.C_M_NAME").alias("c_m_name"),
                            col("Action.Customer.Address.C_ADLINE1").alias("c_adline1"),
                            col("Action.Customer.Address.C_ADLINE2").alias("c_adline2"),
                            col("Action.Customer.Address.C_ZIPCODE").alias("c_zipcode"),
                            col("Action.Customer.Address.C_CITY").alias("c_city"),
                            col("Action.Customer.Address.C_STATE_PROV").alias("c_state_prov"),
                            col("Action.Customer.Address.C_CTRY").alias("c_ctry"),
                            col("Action.Customer.ContactInfo.C_PRIM_EMAIL").alias("c_prim_email"),
                            col("Action.Customer.ContactInfo.C_ALT_EMAIL").alias("c_alt_email"),
                            col("Action.Customer.TaxInfo.C_LCL_TX_ID").alias("c_lcl_tx_id"),
                            col("Action.Customer.TaxInfo.C_NAT_TX_ID").alias("c_nat_tx_id"),
                            col("_batch_id").alias("batch_id"),
                            col("_load_timestamp").alias("load_timestamp"),
                        ).filter(col("Action.Customer._C_ID").isNotNull())
                        
                        if customer_df.count() > 0:
                            logger.info("Pattern 2 (explode array) succeeded")
                            break
            except Exception as e:
                extraction_errors.append(f"Pattern 2: {e}")
        
        if customer_df is None or customer_df.count() == 0:
            raise RuntimeError(f"Failed to extract customers: {extraction_errors}")
        
        # Add SCD Type 2 columns
        silver_df = customer_df.select(
            col("c_id").cast(LongType()).alias("sk_customer_id"),
            col("c_id").cast(LongType()).alias("customer_id"),
            col("c_tax_id").alias("tax_id"),
            col("action_type").alias("status"),
            col("c_l_name").alias("last_name"),
            col("c_f_name").alias("first_name"),
            col("c_m_name").alias("middle_name"),
            when(col("c_gndr").isin("M", "m"), "Male")
                .when(col("c_gndr").isin("F", "f"), "Female")
                .otherwise("Unknown").alias("gender"),
            col("c_tier").cast(IntegerType()).alias("tier"),
            to_date(col("c_dob")).alias("dob"),
            col("c_adline1").alias("address_line1"),
            col("c_adline2").alias("address_line2"),
            col("c_zipcode").alias("postal_code"),
            col("c_city").alias("city"),
            col("c_state_prov").alias("state_prov"),
            col("c_ctry").alias("country"),
            col("c_prim_email").alias("email1"),
            col("c_alt_email").alias("email2"),
            col("c_lcl_tx_id").alias("local_tax_id"),
            col("c_nat_tx_id").alias("national_tax_id"),
            col("batch_id"),
            when(col("action_type") == "INACT", lit(False)).otherwise(lit(True)).alias("is_current"),
            to_timestamp(col("action_ts")).alias("effective_date"),
            lit(None).cast(TimestampType()).alias("end_date"),
            col("load_timestamp"),
        )
        
        # Batch 1: Full historical load (overwrite)
        # Batch 2+: Incremental CDC with SCD Type 2
        if batch_id == 1:
            return self._write_silver_table(silver_df, target_table, batch_id)
        else:
            # Incremental: Apply SCD Type 2 logic
            # This closes out existing current records and inserts new versions
            logger.info(f"Applying SCD Type 2 CDC for batch {batch_id}")
            return self._apply_scd_type2(
                incoming_df=silver_df,
                target_table=target_table,
                key_column="customer_id",
                effective_date_col="effective_date"
            )
