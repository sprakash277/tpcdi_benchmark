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
    col, lit, when, to_date, to_timestamp, explode, current_timestamp, coalesce, trim
)
from pyspark.sql.types import LongType, IntegerType, StringType, DateType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverCustomers(SilverLoaderBase):
    """
    Silver layer loader for Customers.
    
    Handles two different source formats:
    - Batch 1: XML (CustomerMgmt.xml) - event log
    - Batch 2+: Pipe-delimited (Customer.txt) - state snapshot
    
    Implements SCD Type 2 for incremental loads.
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load customers from bronze table.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            batch_id: Batch number (1 = XML, 2+ = pipe-delimited)
            
        Returns:
            DataFrame with cleaned customer data
        """
        logger.info(f"Loading silver_customers from {bronze_table} (batch {batch_id})")
        
        if batch_id == 1:
            # Batch 1: Load from XML
            return self._load_from_xml(bronze_table, target_table, batch_id)
        else:
            # Batch 2+: Load from pipe-delimited
            return self._load_from_pipe_delimited(bronze_table, target_table, batch_id)
    
    def _load_from_xml(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load customers from XML format (Batch 1).
        
        CustomerMgmt.xml structure:
        - Root: <TPCDI:Actions>
        - Rows: <TPCDI:Action> or <Action>
        - Attributes: _ActionType, _ActionTS, _C_ID, _CA_ID
        - Elements: Customer data nested within Action
        
        Args:
            bronze_table: bronze_customer_mgmt table
            target_table: Target silver table
            batch_id: Batch number (should be 1)
            
        Returns:
            DataFrame with cleaned customer data
        """
        logger.info("Parsing customers from XML format (Batch 1)")
        
        bronze_df = self.spark.table(bronze_table)
        bronze_df = bronze_df.filter(col("_batch_id") == batch_id)
        
        # Try multiple XML parsing strategies
        # Bronze schema: Customer (Name, Address, ContactInfo, TaxInfo, _C_*), _ActionType, _ActionTS
        # No Action array - each row is one action. See docs/CUSTOMERMGMT_STRUCTURE.md.
        extraction_errors = []
        customer_df = None
        temp_view = f"_temp_customermgmt_xml_{id(bronze_df)}"
        bronze_df.createOrReplaceTempView(temp_view)
        
        # Strategy 1: Nested structs per TPC-DI (Name, Address, ContactInfo, TaxInfo)
        try:
            logger.info("Attempting XML parse with nested struct paths (Name, Address, ContactInfo, TaxInfo)")
            sql_query = f"""
            SELECT 
                _ActionType as action_type,
                _ActionTS as action_ts,
                Customer._C_ID as c_id,
                Customer._C_TAX_ID as c_tax_id,
                Customer.Name.C_L_NAME as c_l_name,
                Customer.Name.C_F_NAME as c_f_name,
                Customer.Name.C_M_NAME as c_m_name,
                Customer._C_GNDR as c_gndr,
                Customer._C_TIER as c_tier,
                Customer._C_DOB as c_dob,
                Customer.Address.C_ADLINE1 as c_adline1,
                Customer.Address.C_ADLINE2 as c_adline2,
                Customer.Address.C_ZIPCODE as c_zipcode,
                Customer.Address.C_CITY as c_city,
                Customer.Address.C_STATE_PROV as c_state_prov,
                Customer.Address.C_CTRY as c_ctry,
                Customer.ContactInfo.C_PRIM_EMAIL as c_prim_email,
                Customer.ContactInfo.C_ALT_EMAIL as c_alt_email,
                Customer.TaxInfo.C_LCL_TX_ID as c_lcl_tx_id,
                Customer.TaxInfo.C_NAT_TX_ID as c_nat_tx_id,
                _batch_id as batch_id,
                _load_timestamp as load_timestamp
            FROM {temp_view}
            WHERE _ActionType IN ('NEW', 'UPDCUST', 'INACT') AND Customer IS NOT NULL
            """
            
            customer_df = self.spark.sql(sql_query)
            if customer_df.count() > 0:
                logger.info(f"Successfully parsed {customer_df.count()} customer records from XML (nested structs)")
            else:
                raise ValueError("No records extracted")
        except Exception as e:
            extraction_errors.append(f"Strategy 1 (nested structs) failed: {e}")
            logger.warning(f"XML parse (nested structs) failed: {e}")
            customer_df = None
        
        # Strategy 2: Same nested paths with COALESCE for optional structs (Name/Address/ContactInfo/TaxInfo)
        if customer_df is None or customer_df.count() == 0:
            try:
                self.spark.catalog.dropTempView(temp_view)
            except Exception:
                pass
            temp_view = f"_temp_customermgmt_xml2_{id(bronze_df)}"
            bronze_df.createOrReplaceTempView(temp_view)
            try:
                logger.info("Attempting XML parse with COALESCE for optional nested structs")
                sql_query = f"""
                SELECT 
                    _ActionType as action_type,
                    _ActionTS as action_ts,
                    Customer._C_ID as c_id,
                    TRIM(COALESCE(Customer._C_TAX_ID, '')) as c_tax_id,
                    TRIM(COALESCE(Customer.Name.C_L_NAME, '')) as c_l_name,
                    TRIM(COALESCE(Customer.Name.C_F_NAME, '')) as c_f_name,
                    TRIM(COALESCE(Customer.Name.C_M_NAME, '')) as c_m_name,
                    TRIM(COALESCE(Customer._C_GNDR, '')) as c_gndr,
                    TRIM(COALESCE(CAST(Customer._C_TIER AS STRING), '0')) as c_tier,
                    TRIM(COALESCE(Customer._C_DOB, '')) as c_dob,
                    TRIM(COALESCE(Customer.Address.C_ADLINE1, '')) as c_adline1,
                    TRIM(COALESCE(Customer.Address.C_ADLINE2, '')) as c_adline2,
                    TRIM(COALESCE(Customer.Address.C_ZIPCODE, '')) as c_zipcode,
                    TRIM(COALESCE(Customer.Address.C_CITY, '')) as c_city,
                    TRIM(COALESCE(Customer.Address.C_STATE_PROV, '')) as c_state_prov,
                    TRIM(COALESCE(Customer.Address.C_CTRY, '')) as c_ctry,
                    TRIM(COALESCE(Customer.ContactInfo.C_PRIM_EMAIL, '')) as c_prim_email,
                    TRIM(COALESCE(Customer.ContactInfo.C_ALT_EMAIL, '')) as c_alt_email,
                    TRIM(COALESCE(Customer.TaxInfo.C_LCL_TX_ID, '')) as c_lcl_tx_id,
                    TRIM(COALESCE(Customer.TaxInfo.C_NAT_TX_ID, '')) as c_nat_tx_id,
                    _batch_id as batch_id,
                    _load_timestamp as load_timestamp
                FROM {temp_view}
                WHERE _ActionType IN ('NEW', 'UPDCUST', 'INACT') AND Customer IS NOT NULL
                """
                customer_df = self.spark.sql(sql_query)
                if customer_df.count() > 0:
                    logger.info(f"Successfully parsed {customer_df.count()} customer records (COALESCE fallback)")
                else:
                    raise ValueError("No records extracted")
            except Exception as e:
                extraction_errors.append(f"Strategy 2 (COALESCE) failed: {e}")
                logger.warning(f"XML parse (COALESCE) failed: {e}")
                customer_df = None
        
        try:
            self.spark.catalog.dropTempView(temp_view)
        except Exception:
            pass
        
        if customer_df is None or customer_df.count() == 0:
            raise RuntimeError(f"Failed to extract customers: {extraction_errors}")
        
        # Transform to silver schema
        silver_df = self._transform_to_silver_schema(
            customer_df,
            has_action_type=True,
            batch_id=batch_id
        )
        
        # Batch 1: Full historical load (overwrite)
        return self._write_silver_table(silver_df, target_table, batch_id)
    
    def _load_from_pipe_delimited(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load customers from pipe-delimited format (Batch 2+).
        
        Customer.txt format (pipe-delimited, state-at-time snapshot):
        I|C_ID|BROKER_ID|C_TAX_ID|C_ST_ID|C_L_NAME|C_F_NAME|C_M_NAME|C_GNDR|C_TIER|C_DOB|
        C_ADLINE1|C_ADLINE2|C_ZIPCODE|C_CITY|C_STATE_PROV|C_CTRY|...|C_PRIM_EMAIL|C_ALT_EMAIL|C_LCL_TX_ID|C_NAT_TX_ID
        
        Actual column positions (0-indexed after parsing):
        - _c0: Record type ('I'=Insert/Incremental, 'U'=Update, 'D'=Delete)
          Used for SCD Type 2 identification: 'D' or C_ST_ID='INACT' → INACT, otherwise UPDCUST.
          NEW vs UPDCUST is determined during SCD2 MERGE by checking if customer_id exists in target table.
        - _c1: C_ID
        - _c2: BROKER_ID (ignored)
        - _c3: C_TAX_ID
        - _c4: C_ST_ID
        - _c5: C_L_NAME
        - _c6: C_F_NAME
        - _c7: C_M_NAME
        - _c8: C_GNDR
        - _c9: C_TIER
        - _c10: C_DOB
        - _c11: C_ADLINE1
        - _c12: C_ADLINE2
        - _c13: C_ZIPCODE
        - _c14: C_CITY
        - _c15: C_STATE_PROV
        - _c16: C_CTRY
        - _c30: C_PRIM_EMAIL
        - _c31: C_ALT_EMAIL
        - _c32: C_LCL_TX_ID
        - _c33: C_NAT_TX_ID
        
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
        
        # Parse pipe-delimited (parse up to 35 columns to be safe)
        # TPC-DI Customer.txt format based on sample:
        # I|C_ID|BROKER_ID|C_TAX_ID|C_ST_ID|C_L_NAME|C_F_NAME|C_M_NAME|C_GNDR|C_TIER|C_DOB|
        # C_ADLINE1|C_ADLINE2|C_ZIPCODE|C_CITY|C_STATE_PROV|C_CTRY|...|C_PRIM_EMAIL|C_ALT_EMAIL|C_LCL_TX_ID|C_NAT_TX_ID
        # Total: ~33 columns (including record type 'I' at position 0)
        parsed_df = self._parse_pipe_delimited(bronze_df, 35)
        
        # Check actual column count by inspecting schema
        actual_cols = [c for c in parsed_df.columns if c.startswith("_c")]
        max_col_idx = max([int(c[2:]) for c in actual_cols]) if actual_cols else -1
        logger.info(f"Customer.txt parsed with {max_col_idx + 1} columns (indices 0-{max_col_idx})")
        
        # Helper to check if column exists
        def col_exists(idx: int) -> bool:
            return f"_c{idx}" in parsed_df.columns
        
        # Map to customer schema with safe column access
        # Customer.txt format based on actual sample records:
        # I|C_ID|BROKER_ID|C_TAX_ID|C_ST_ID|C_L_NAME|C_F_NAME|C_M_NAME|C_GNDR|C_TIER|C_DOB|
        # C_ADLINE1|C_ADLINE2|C_ZIPCODE|C_CITY|C_STATE_PROV|C_CTRY|...|C_PRIM_EMAIL|C_ALT_EMAIL|C_LCL_TX_ID|C_NAT_TX_ID
        # Column mapping (0-indexed after parsing):
        # _c0: Record type 'I' (skip)
        # _c1: C_ID
        # _c2: BROKER_ID (skip, not used in customer schema)
        # _c3: C_TAX_ID
        # _c4: C_ST_ID
        # _c5: C_L_NAME
        # _c6: C_F_NAME
        # _c7: C_M_NAME
        # _c8: C_GNDR
        # _c9: C_TIER
        # _c10: C_DOB
        # _c11: C_ADLINE1
        # _c12: C_ADLINE2
        # _c13: C_ZIPCODE
        # _c14: C_CITY
        # _c15: C_STATE_PROV
        # _c16: C_CTRY
        # _c17-29: Various fields (phone numbers, etc.) - skip
        # _c30: C_PRIM_EMAIL
        # _c31: C_ALT_EMAIL
        # _c32: C_LCL_TX_ID
        # _c33: C_NAT_TX_ID
        
        # Build select list dynamically based on available columns
        select_cols = []
        
        # Capture record type (_c0) for CDC identification
        # Record types: 'I' = Insert/Incremental, 'U' = Update, 'D' = Delete
        # This helps identify the operation type for SCD Type 2 processing
        if col_exists(0):
            select_cols.append(coalesce(col("_c0"), lit("I")).alias("record_type"))
        else:
            select_cols.append(lit("I").alias("record_type"))  # Default to 'I' if missing
        
        # Map columns based on actual format
        field_mappings = [
            (1, "c_id", LongType, True),      # C_ID
            (3, "c_tax_id", None, False),     # C_TAX_ID
            (4, "c_st_id", None, False),      # C_ST_ID
            (5, "c_l_name", None, False),     # C_L_NAME
            (6, "c_f_name", None, False),     # C_F_NAME
            (7, "c_m_name", None, False),     # C_M_NAME
            (8, "c_gndr", None, False),       # C_GNDR
            (9, "c_tier", IntegerType, True),  # C_TIER
            (10, "c_dob", None, False),       # C_DOB
            (11, "c_adline1", None, False),   # C_ADLINE1
            (12, "c_adline2", None, False),   # C_ADLINE2
            (13, "c_zipcode", None, False),  # C_ZIPCODE
            (14, "c_city", None, False),      # C_CITY
            (15, "c_state_prov", None, False), # C_STATE_PROV
            (16, "c_ctry", None, False),     # C_CTRY
            (30, "c_prim_email", None, False), # C_PRIM_EMAIL
            (31, "c_alt_email", None, False), # C_ALT_EMAIL
            (32, "c_lcl_tx_id", None, False), # C_LCL_TX_ID
            (33, "c_nat_tx_id", None, False), # C_NAT_TX_ID
        ]
        
        for idx, alias_name, cast_type, is_numeric in field_mappings:
            if col_exists(idx):
                if cast_type:
                    default_val = "0" if is_numeric else ""
                    select_cols.append(coalesce(col(f"_c{idx}"), lit(default_val)).cast(cast_type()).alias(alias_name))
                else:
                    select_cols.append(coalesce(col(f"_c{idx}"), lit("")).alias(alias_name))
            else:
                # Column doesn't exist, use default
                if cast_type:
                    select_cols.append(lit("0").cast(cast_type()).alias(alias_name))
                else:
                    select_cols.append(lit("").alias(alias_name))
        
        # Add metadata columns
        select_cols.extend([
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        ])
        
        customer_df = parsed_df.select(*select_cols)
        
        # Determine action_type based on record type and status for SCD Type 2 identification
        # 
        # Record Type Meanings:
        # - 'I' = Insert/Incremental (state snapshot - could be NEW or UPDCUST)
        # - 'U' = Update (explicit update)
        # - 'D' = Delete (inactivation)
        #
        # SCD Type 2 Logic:
        # - NEW vs UPDCUST: Determined during MERGE by checking if customer_id exists in target table
        #   - If customer_id doesn't exist → NEW (insert as new record)
        #   - If customer_id exists → UPDCUST (close existing record, insert new version)
        # - INACT: Determined by record_type='D' OR c_st_id='INACT' (close existing record)
        #
        # The record_type field is captured for audit/debugging but the SCD2 merge logic
        # handles NEW vs UPDCUST automatically based on key existence.
        customer_df = customer_df.withColumn(
            "action_type",
            when(
                (col("record_type") == "D") | (col("c_st_id") == "INACT"),
                lit("INACT")
            ).when(
                col("record_type") == "U",
                lit("UPDCUST")
            ).when(
                col("record_type") == "I",
                # 'I' could be NEW or UPDCUST - will be determined during SCD2 merge
                # Default to UPDCUST for now (SCD2 logic will handle correctly)
                lit("UPDCUST")
            ).otherwise(
                lit("UPDCUST")  # Default fallback
            )
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
        Transform customer DataFrame to silver schema.
        
        Args:
            customer_df: Input DataFrame with customer fields
            has_action_type: Whether action_type/action_ts columns exist
            batch_id: Batch number
            
        Returns:
            DataFrame with silver schema
        """
        # Build select columns dynamically based on whether action_type exists
        select_cols = []
        
        # Core customer fields
        select_cols.extend([
            col("c_id").cast(LongType()).alias("customer_id"),
            coalesce(col("c_tax_id"), lit("")).alias("tax_id"),
            coalesce(col("c_l_name"), lit("")).alias("last_name"),
            coalesce(col("c_f_name"), lit("")).alias("first_name"),
            coalesce(col("c_m_name"), lit("")).alias("middle_name"),
            coalesce(col("c_gndr"), lit("")).alias("gender"),
            coalesce(col("c_tier"), lit("0")).cast(IntegerType()).alias("tier"),
            # Parse dob as date, handling empty strings and nulls
            when(
                (col("c_dob").isNull()) | (trim(col("c_dob")) == ""),
                lit(None).cast(DateType())
            ).otherwise(
                to_date(col("c_dob"), "yyyy-MM-dd")
            ).alias("dob"),
            coalesce(col("c_adline1"), lit("")).alias("address_line1"),
            coalesce(col("c_adline2"), lit("")).alias("address_line2"),
            coalesce(col("c_zipcode"), lit("")).alias("zipcode"),
            coalesce(col("c_city"), lit("")).alias("city"),
            coalesce(col("c_state_prov"), lit("")).alias("state_prov"),
            coalesce(col("c_ctry"), lit("")).alias("country"),
            coalesce(col("c_prim_email"), lit("")).alias("primary_email"),
            coalesce(col("c_alt_email"), lit("")).alias("alternate_email"),
            coalesce(col("c_lcl_tx_id"), lit("")).alias("local_tax_id"),
            coalesce(col("c_nat_tx_id"), lit("")).alias("national_tax_id"),
        ])
        
        # Action fields (conditional)
        if has_action_type:
            select_cols.extend([
                col("action_type").alias("action_type"),
                to_timestamp(col("action_ts")).alias("action_timestamp"),
            ])
        else:
            # Default values if not present
            select_cols.extend([
                lit("NEW").alias("action_type"),
                current_timestamp().alias("action_timestamp"),
            ])
        
        # SCD Type 2 fields
        if has_action_type:
            # Use action_ts as effective_date
            select_cols.append(
                to_date(to_timestamp(col("action_ts"))).alias("effective_date")
            )
        else:
            # Use current date
            select_cols.append(
                to_date(current_timestamp()).alias("effective_date")
            )
        
        select_cols.extend([
            lit(None).cast(DateType()).alias("end_date"),
            lit(True).alias("is_current"),
            col("batch_id").cast(IntegerType()).alias("batch_id"),
            col("load_timestamp").alias("load_timestamp"),
        ])
        
        silver_df = customer_df.select(*select_cols)
        
        return silver_df
