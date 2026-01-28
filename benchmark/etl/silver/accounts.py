"""
Silver layer loader for Accounts.

Parses and cleans account data from bronze_customer_mgmt (Batch 1) or bronze_account (Batch 2+).
Implements SCD Type 2 for CDC (Change Data Capture) on incremental loads.

TPC-DI Format Differences:
- Batch 1: CustomerMgmt.xml (XML event log)
- Batch 2+: Account.txt (pipe-delimited state snapshot)
"""

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, when, to_date, to_timestamp, explode, current_timestamp, coalesce
)
from pyspark.sql.types import LongType, IntegerType, StringType, DateType, TimestampType

from benchmark.etl.silver.base import SilverLoaderBase

logger = logging.getLogger(__name__)


class SilverAccounts(SilverLoaderBase):
    """
    Silver layer loader for Accounts.
    
    Handles two different source formats:
    - Batch 1: XML (CustomerMgmt.xml) - event log
    - Batch 2+: Pipe-delimited (Account.txt) - state snapshot
    
    Implements SCD Type 2 for incremental loads.
    """
    
    def load(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load accounts from bronze table.
        
        Args:
            bronze_table: Source bronze table name
            target_table: Target silver table name
            batch_id: Batch number (1 = XML, 2+ = pipe-delimited)
            
        Returns:
            DataFrame with cleaned account data
        """
        logger.info(f"Loading silver_accounts from {bronze_table} (batch {batch_id})")
        
        if batch_id == 1:
            # Batch 1: Load from XML
            return self._load_from_xml(bronze_table, target_table, batch_id)
        else:
            # Batch 2+: Load from pipe-delimited
            return self._load_from_pipe_delimited(bronze_table, target_table, batch_id)
    
    def _load_from_xml(self, bronze_table: str, target_table: str, batch_id: int) -> DataFrame:
        """
        Load accounts from XML format (Batch 1).
        
        CustomerMgmt.xml structure:
        - Root: <TPCDI:Actions>
        - Rows: <TPCDI:Action> or <Action>
        - Attributes: _ActionType, _ActionTS, _C_ID, _CA_ID
        - Elements: Account data nested within Action/Customer
        
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
        
        # Try multiple XML parsing strategies
        extraction_errors = []
        account_df = None
        
        # Strategy 1: TPCDI:Action with rootTag
        try:
            logger.info("Attempting XML parse with TPCDI:Action and rootTag")
            temp_view = f"_temp_accountmgmt_xml_{id(bronze_df)}"
            bronze_df.createOrReplaceTempView(temp_view)
            
            sql_query = f"""
            SELECT 
                _ActionType as action_type,
                _ActionTS as action_ts,
                _CA_ID as ca_id,
                Customer.Account._CA_B_ID as ca_b_id,
                Customer._C_ID as c_id,
                Customer.Account.CA_NAME as ca_name,
                Customer.Account._CA_TAX_ST as ca_tax_st,
                Customer.Account._CA_ST_ID as ca_st_id,
                _batch_id as batch_id,
                _load_timestamp as load_timestamp
            FROM {temp_view}
            WHERE _ActionType IN ('ADDACCT', 'UPDACCT', 'INACT')
            """
            
            account_df = self.spark.sql(sql_query)
            if account_df.count() > 0:
                logger.info(f"Successfully parsed {account_df.count()} account records from XML")
            else:
                raise ValueError("No records extracted")
        except Exception as e:
            extraction_errors.append(f"Strategy 1 failed: {e}")
            logger.warning(f"XML parsing strategy 1 failed: {e}")
            try:
                self.spark.catalog.dropTempView(temp_view)
            except:
                pass
        
        # Strategy 2: Try with explode if Action is an array
        if account_df is None or account_df.count() == 0:
            try:
                logger.info("Attempting XML parse with explode")
                temp_view = f"_temp_accountmgmt_xml2_{id(bronze_df)}"
                bronze_df.createOrReplaceTempView(temp_view)
                
                sql_query = f"""
                SELECT 
                    explode(Action) as action_row
                FROM {temp_view}
                """
                exploded_df = self.spark.sql(sql_query)
                exploded_df.createOrReplaceTempView("exploded_actions")
                
                sql_query2 = """
                SELECT 
                    action_row._ActionType as action_type,
                    action_row._ActionTS as action_ts,
                    action_row._CA_ID as ca_id,
                    action_row.Customer.Account._CA_B_ID as ca_b_id,
                    action_row.Customer._C_ID as c_id,
                    action_row.Customer.Account.CA_NAME as ca_name,
                    action_row.Customer.Account._CA_TAX_ST as ca_tax_st,
                    action_row.Customer.Account._CA_ST_ID as ca_st_id
                FROM exploded_actions
                WHERE action_row._ActionType IN ('ADDACCT', 'UPDACCT', 'INACT')
                """
                
                account_df = self.spark.sql(sql_query2)
                if account_df.count() > 0:
                    logger.info(f"Successfully parsed {account_df.count()} account records with explode")
                else:
                    raise ValueError("No records extracted")
            except Exception as e:
                extraction_errors.append(f"Strategy 2 failed: {e}")
                logger.warning(f"XML parsing strategy 2 failed: {e}")
                try:
                    self.spark.catalog.dropTempView(temp_view)
                    self.spark.catalog.dropTempView("exploded_actions")
                except:
                    pass
        
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
        I|CA_ID|CA_B_ID|CA_C_ID|UNKNOWN_ID|CA_NAME|CA_TAX_ST|CA_ST_ID
        
        Actual column positions (0-indexed after parsing):
        - _c0: Record type ('I'=Insert/Incremental, 'U'=Update, 'D'=Delete)
          Used for SCD Type 2 identification: 'D' or CA_ST_ID='INACT' → INACT, otherwise UPDACCT.
          NEW vs UPDACCT is determined during SCD2 MERGE by checking if account_id exists in target table.
        - _c1: CA_ID
        - _c2: CA_B_ID (broker ID)
        - _c3: CA_C_ID (customer ID)
        - _c4: UNKNOWN_ID (ignored, not used in account schema)
        - _c5: CA_NAME
        - _c6: CA_TAX_ST
        - _c7: CA_ST_ID
        
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
        
        # Parse pipe-delimited (parse up to 10 columns to be safe)
        # TPC-DI Account.txt format based on actual sample records:
        # I|CA_ID|CA_B_ID|CA_C_ID|UNKNOWN_ID|CA_NAME|CA_TAX_ST|CA_ST_ID
        # Total: 8 columns (including record type 'I' at position 0)
        parsed_df = self._parse_pipe_delimited(bronze_df, 10)
        
        # Check actual column count
        actual_cols = [c for c in parsed_df.columns if c.startswith("_c")]
        max_col_idx = max([int(c[2:]) for c in actual_cols]) if actual_cols else -1
        logger.info(f"Account.txt parsed with {max_col_idx + 1} columns (indices 0-{max_col_idx})")
        
        # Helper to check if column exists
        def col_exists(idx: int) -> bool:
            return f"_c{idx}" in parsed_df.columns
        
        # Map to account schema with safe column access
        # Account.txt format based on actual sample records:
        # I|CA_ID|CA_B_ID|CA_C_ID|UNKNOWN_ID|CA_NAME|CA_TAX_ST|CA_ST_ID
        # Column mapping (0-indexed after parsing):
        # _c0: Record type ('I'=Insert/Incremental, 'U'=Update, 'D'=Delete)
        # _c1: CA_ID
        # _c2: CA_B_ID (broker ID)
        # _c3: CA_C_ID (customer ID)
        # _c4: UNKNOWN_ID (ignored, not used in account schema)
        # _c5: CA_NAME
        # _c6: CA_TAX_ST
        # _c7: CA_ST_ID
        
        # Build select list dynamically
        select_cols = []
        
        # Capture record type (_c0) for CDC identification
        if col_exists(0):
            select_cols.append(coalesce(col("_c0"), lit("I")).alias("record_type"))
        else:
            select_cols.append(lit("I").alias("record_type"))  # Default to 'I' if missing
        
        # Required fields (mapped based on actual format)
        field_mappings = [
            (1, "ca_id", LongType, True),      # CA_ID
            (2, "ca_b_id", LongType, True),    # CA_B_ID
            (3, "c_id", LongType, True),       # CA_C_ID (customer ID)
            (5, "ca_name", None, False),       # CA_NAME (skip _c4)
            (6, "ca_tax_st", IntegerType, True), # CA_TAX_ST
            (7, "ca_st_id", None, False),      # CA_ST_ID
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
        
        account_df = parsed_df.select(*select_cols)
        
        # Determine action_type based on record type and status for SCD Type 2 identification
        # 
        # Record Type Meanings:
        # - 'I' = Insert/Incremental (state snapshot - could be NEW or UPDACCT)
        # - 'U' = Update (explicit update)
        # - 'D' = Delete (inactivation)
        #
        # SCD Type 2 Logic:
        # - NEW vs UPDACCT: Determined during MERGE by checking if account_id exists in target table
        #   - If account_id doesn't exist → NEW (insert as new record)
        #   - If account_id exists → UPDACCT (close existing record, insert new version)
        # - INACT: Determined by record_type='D' OR ca_st_id='INACT' (close existing record)
        #
        # The record_type field is captured for audit/debugging but the SCD2 merge logic
        # handles NEW vs UPDACCT automatically based on key existence.
        account_df = account_df.withColumn(
            "action_type",
            when(
                (col("record_type") == "D") | (col("ca_st_id") == "INACT"),
                lit("INACT")
            ).when(
                col("record_type") == "U",
                lit("UPDACCT")
            ).when(
                col("record_type") == "I",
                # 'I' could be NEW or UPDACCT - will be determined during SCD2 merge
                # Default to UPDACCT for now (SCD2 logic will handle correctly)
                lit("UPDACCT")
            ).otherwise(
                lit("UPDACCT")  # Default fallback
            )
        ).withColumn(
            "action_ts",
            current_timestamp()  # Use current timestamp for state snapshot
        )
        
        # Transform to silver schema
        silver_df = self._transform_to_silver_schema(
            account_df,
            has_action_type=True,
            batch_id=batch_id
        )
        
        # Batch 2+: Incremental CDC with SCD Type 2
        logger.info(f"Applying SCD Type 2 CDC for batch {batch_id}")
        return self._apply_scd_type2(
            incoming_df=silver_df,
            target_table=target_table,
            key_column="account_id",
            effective_date_col="effective_date"
        )
    
    def _transform_to_silver_schema(self, account_df: DataFrame, 
                                    has_action_type: bool, batch_id: int) -> DataFrame:
        """
        Transform account DataFrame to silver schema.
        
        Args:
            account_df: Input DataFrame with account fields
            has_action_type: Whether action_type/action_ts columns exist
            batch_id: Batch number
            
        Returns:
            DataFrame with silver schema
        """
        # Build select columns dynamically based on whether action_type exists
        select_cols = []
        
        # Core account fields
        select_cols.extend([
            col("ca_id").cast(LongType()).alias("account_id"),
            coalesce(col("ca_b_id"), lit("0")).cast(LongType()).alias("broker_id"),
            coalesce(col("c_id"), lit("0")).cast(LongType()).alias("customer_id"),
            coalesce(col("ca_name"), lit("")).alias("account_name"),
            coalesce(col("ca_tax_st"), lit("0")).cast(IntegerType()).alias("tax_status"),
            coalesce(col("ca_st_id"), lit("")).alias("status_id"),
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
                lit("ADDACCT").alias("action_type"),
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
        
        silver_df = account_df.select(*select_cols)
        
        return silver_df
