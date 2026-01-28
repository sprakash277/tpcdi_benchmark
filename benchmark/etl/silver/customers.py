"""
Silver layer loader for Customers.

Parses and cleans customer data from bronze_customer_mgmt.
Implements SCD Type 2 for CDC (Change Data Capture) on incremental loads.

TPC-DI Format Differences:
- Batch 1: CustomerMgmt.xml (XML event log)
- Batch 2+: Customer.txt (pipe-delimited state snapshot)
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, to_date, to_timestamp, explode, current_timestamp
)
from pyspark.sql.types import LongType, IntegerType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverCustomers(SilverLoaderBase):
    """
    Silver layer loader for Customers.
    
    Handles two different formats per TPC-DI spec:
    - Batch 1: CustomerMgmt.xml (XML event log)
    - Batch 2+: Customer.txt (pipe-delimited state snapshot)
    
    CDC/SCD Type 2 Handling:
    - Batch 1 (Historical): Full load, overwrite from XML
    - Batch 2+ (Incremental): 
      - Parse pipe-delimited Customer.txt (state-at-time snapshot)
      - Apply SCD Type 2: Close existing records, insert new versions
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Parse and clean customer data.
        
        Args:
            bronze_table: Source bronze table name
              - Batch 1: bronze_customer_mgmt (XML)
              - Batch 2+: bronze_customer (pipe-delimited)
            target_table: Target silver table name
            batch_id: Batch number
            
        Returns:
            DataFrame with cleaned customer data
        """
        logger.info(f"Loading silver_customers from {bronze_table} (batch {batch_id})")
        
        if batch_id == 1:
            # Batch 1: Parse XML from CustomerMgmt.xml
            return self._load_from_xml(bronze_table, target_table, batch_id)
        else:
            # Batch 2+: Parse pipe-delimited from Customer.txt
            return self._load_from_pipe_delimited(bronze_table, target_table, batch_id)
    
    def _load_from_xml(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load customers from XML format (Batch 1).
        
        Args:
            bronze_table: bronze_customer_mgmt table
            target_table: Target silver table
            batch_id: Batch number (should be 1)
            
        Returns:
            DataFrame with cleaned customer data
        """
        logger.info("Parsing customers from XML format (Batch 1)")
        
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
        
        # Transform to silver schema
        silver_df = self._transform_to_silver_schema(
            customer_df, 
            has_action_type=True,  # XML has ActionType
            batch_id=batch_id
        )
        
        # Batch 1: Full historical load (overwrite)
        return self._write_silver_table(silver_df, target_table, batch_id)
    
    def _load_from_pipe_delimited(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load customers from pipe-delimited format (Batch 2+).
        
        Customer.txt format (pipe-delimited, state-at-time snapshot):
        C_ID|C_TAX_ID|C_ST_ID|C_L_NAME|C_F_NAME|C_M_NAME|C_GNDR|C_TIER|C_DOB|
        C_ADLINE1|C_ADLINE2|C_ZIPCODE|C_CITY|C_STATE_PROV|C_CTRY|
        C_PRIM_EMAIL|C_ALT_EMAIL|C_PHONE_1|C_PHONE_2|C_PHONE_3|
        C_LCL_TX_ID|C_NAT_TX_ID
        
        Args:
            bronze_table: bronze_customer table
            target_table: Target silver table
            batch_id: Batch number (should be 2+)
            
        Returns:
            DataFrame with cleaned customer data
        """
        logger.info("Parsing customers from pipe-delimited format (Batch 2+)")
        
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Parse pipe-delimited (approximately 20 columns based on TPC-DI spec)
        # Note: Exact column count may vary, but we'll parse common fields
        parsed_df = self._parse_pipe_delimited(bronze_df, 20)
        
        # Map to customer schema
        # Customer.txt is a state snapshot, so all records are "current" updates
        customer_df = parsed_df.select(
            col("_c0").cast(LongType()).alias("c_id"),
            col("_c1").alias("c_tax_id"),
            col("_c2").alias("c_st_id"),  # Status ID (not in XML)
            col("_c3").alias("c_l_name"),
            col("_c4").alias("c_f_name"),
            col("_c5").alias("c_m_name"),
            col("_c6").alias("c_gndr"),
            col("_c7").cast(IntegerType()).alias("c_tier"),
            col("_c8").alias("c_dob"),
            col("_c9").alias("c_adline1"),
            col("_c10").alias("c_adline2"),
            col("_c11").alias("c_zipcode"),
            col("_c12").alias("c_city"),
            col("_c13").alias("c_state_prov"),
            col("_c14").alias("c_ctry"),
            col("_c15").alias("c_prim_email"),
            col("_c16").alias("c_alt_email"),
            # Phone fields (_c17, _c18, _c19) - not used in silver schema
            col("_c20").alias("c_lcl_tx_id"),  # Adjust indices if needed
            col("_c21").alias("c_nat_tx_id"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        
        # Add action_type and action_ts for state snapshot
        # State snapshots are treated as "UPDCUST" (update) unless status indicates inactive
        customer_df = customer_df.withColumn(
            "action_type",
            when(col("c_st_id") == "INACT", lit("INACT")).otherwise(lit("UPDCUST"))
        ).withColumn(
            "action_ts",
            current_timestamp()  # Use current timestamp for state snapshot
        )
        
        # Transform to silver schema
        silver_df = self._transform_to_silver_schema(
            customer_df,
            has_action_type=True,
            batch_id=batch_id
        )
        
        # Batch 2+: Incremental CDC with SCD Type 2
        logger.info(f"Applying SCD Type 2 CDC for batch {batch_id}")
        return self._apply_scd_type2(
            incoming_df=silver_df,
            target_table=target_table,
            key_column="customer_id",
            effective_date_col="effective_date"
        )
    
    def _transform_to_silver_schema(self, customer_df: DataFrame, 
                                    has_action_type: bool, batch_id: int) -> DataFrame:
        """
        Transform customer data to silver schema.
        
        Args:
            customer_df: DataFrame with customer fields
            has_action_type: Whether action_type and action_ts columns exist
            batch_id: Batch number
            
        Returns:
            DataFrame with silver schema
        """
        # Build select list conditionally
        select_cols = [
            col("c_id").cast(LongType()).alias("sk_customer_id"),
            col("c_id").cast(LongType()).alias("customer_id"),
            col("c_tax_id").alias("tax_id"),
        ]
        
        # Status column
        if has_action_type:
            select_cols.append(col("action_type").alias("status"))
        else:
            select_cols.append(lit("ACTIVE").alias("status"))
        
        select_cols.extend([
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
        ])
        
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
        
        return customer_df.select(*select_cols)
