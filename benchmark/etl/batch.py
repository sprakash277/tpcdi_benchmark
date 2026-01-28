"""
Batch ETL transformations for TPC-DI benchmark.
Handles historical load and initial batch processing.
"""

import logging
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, lit, current_timestamp, split, element_at, size
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.functions import udf

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
    
    def _read_pipe_delimited_txt(self, batch_id: int, file_pattern: str, expected_cols: int) -> DataFrame:
        """
        Read a pipe-delimited .txt file using pure SQL split.
        
        This method bypasses Spark's CSV reader which has issues with pipe delimiter
        on some Databricks runtimes. It uses spark.sql() with a temporary view to avoid
        query context tracking issues.
        
        Args:
            batch_id: Batch number
            file_pattern: File pattern (e.g., "Date.txt")
            expected_cols: Expected number of columns
        
        Returns:
            DataFrame with columns _c0, _c1, _c2, etc.
        """
        logger.info(f"[DEBUG] _read_pipe_delimited_txt: Reading {file_pattern} from Batch{batch_id}")
        
        # Read file as text (single column 'value' with full line content)
        text_df = self.platform.read_batch_files(
            batch_id,
            file_pattern,
            format="text"
        )
        
        # Show sample raw content for debugging
        logger.info(f"[DEBUG] Raw content of {file_pattern} (first 3 lines):")
        text_df.show(3, truncate=False)
        
        # Create temporary view for SQL execution
        temp_view_name = f"_temp_txt_{file_pattern.replace('.', '_').replace('/', '_')}_{batch_id}"
        text_df.createOrReplaceTempView(temp_view_name)
        
        # Build SQL SELECT with split() and element_at() - uses 1-based indexing
        # Note: In Spark SQL, split() uses regex, so pipe needs to be escaped as '\\|'
        # But in SQL string literals, we need '\\|' which becomes '|' after SQL parsing
        # Actually, Spark SQL split() may treat '|' as literal, so try without escaping first
        select_parts = []
        for i in range(expected_cols):
            # Try using '|' directly - Spark SQL split() might handle it as literal delimiter
            # If that doesn't work, we can try '\\|' for regex escaping
            select_parts.append(f"TRIM(COALESCE(element_at(split(value, '|'), {i+1}), '')) AS _c{i}")
        
        sql_query = f"SELECT {', '.join(select_parts)} FROM {temp_view_name}"
        logger.info(f"[DEBUG] Executing SQL: {sql_query[:150]}...")
        
        # Execute SQL and get result
        try:
            df = self.spark.sql(sql_query)
        except Exception as sql_error:
            logger.warning(f"[DEBUG] SQL query failed with '|', trying with escaped '\\|': {sql_error}")
            # Retry with escaped pipe character
            select_parts = []
            for i in range(expected_cols):
                select_parts.append(f"TRIM(COALESCE(element_at(split(value, '\\\\|'), {i+1}), '')) AS _c{i}")
            sql_query = f"SELECT {', '.join(select_parts)} FROM {temp_view_name}"
            df = self.spark.sql(sql_query)
        
        # Drop temporary view
        try:
            self.spark.catalog.dropTempView(temp_view_name)
        except Exception as e:
            logger.warning(f"[DEBUG] Could not drop temp view {temp_view_name}: {e}")
        
        # Debug output - wrap in try-except to avoid errors during display
        logger.info(f"[DEBUG] _read_pipe_delimited_txt: Got {len(df.columns)} columns: {df.columns}")
        try:
            logger.info(f"[DEBUG] Sample rows after split:")
            df.show(3, truncate=50)
        except Exception as show_error:
            logger.warning(f"[DEBUG] Could not display sample rows: {show_error}")
            # Try to get row count instead
            try:
                row_count = df.count()
                logger.info(f"[DEBUG] DataFrame has {row_count} rows")
            except Exception as count_error:
                logger.warning(f"[DEBUG] Could not count rows: {count_error}")
        
        return df
    
    def _read_file_with_delimiter_detection(self, batch_id: int, file_pattern: str, 
                                           expected_cols: int, expected_format: str,
                                           preferred_delimiter: str = "|", **options) -> DataFrame:
        """
        Read a file trying different delimiters if the preferred one doesn't work.
        
        TPC-DI file formats:
        - Most .txt files: Pipe-delimited (|)
        - HR.csv, Prospect.csv: Comma-delimited (,)
        - FINWIRE files: Fixed-width (no delimiter)
        - CustomerMgmt.xml: XML format
        
        Args:
            batch_id: Batch number
            file_pattern: File pattern to read
            expected_cols: Expected minimum columns
            expected_format: Expected format description
            preferred_delimiter: Preferred delimiter (| or ,)
            **options: Additional read options
        
        Returns:
            DataFrame with the file data
        """
        # For .txt files (pipe-delimited per TPC-DI spec), use SQL-based split directly
        # Spark's CSV reader has issues with pipe delimiter on some Databricks runtimes
        if file_pattern.endswith(".txt") and preferred_delimiter == "|":
            logger.info(f"[DEBUG] Reading {file_pattern} as pipe-delimited .txt file using SQL split")
            return self._read_pipe_delimited_txt(batch_id, file_pattern, expected_cols)
        
        # First, read as raw text to inspect actual content
        try:
            # Read the file path to inspect raw content
            file_path = f"Batch{batch_id}/{file_pattern}"
            raw_df = self.platform.read_raw_file(
                file_path,
                format="text",  # Read as raw text first
            )
            logger.info(f"[DEBUG] Raw content of {file_pattern} (first 5 lines):")
            raw_df.show(5, truncate=False)
            
            # Get first row to analyze
            first_row = raw_df.first()
            if first_row:
                # text format returns a single column named 'value'
                raw_line = first_row.value if hasattr(first_row, 'value') else (first_row[0] if len(first_row) > 0 else str(first_row))
                logger.info(f"[DEBUG] First line content: '{raw_line}'")
                logger.info(f"[DEBUG] First line length: {len(str(raw_line))}")
                logger.info(f"[DEBUG] Contains '|': {str(raw_line).count('|')}")
                logger.info(f"[DEBUG] Contains ',': {str(raw_line).count(',')}")
                logger.info(f"[DEBUG] Contains tab: {str(raw_line).count(chr(9))}")
                logger.info(f"[DEBUG] First 100 chars: '{str(raw_line)[:100]}'")
        except Exception as e:
            logger.warning(f"[DEBUG] Could not read raw text for inspection: {e}")
            logger.info(f"[DEBUG] Will proceed with delimiter detection anyway")
        
        # Try preferred delimiter first (for .csv files)
        try:
            # For pipe delimiter, use both sep and delimiter options
            # Note: format is handled by read_batch_files, don't include it here
            read_options = {
                "sep": preferred_delimiter,
                "delimiter": preferred_delimiter,  # Set both sep and delimiter
                "header": False,
                "inferSchema": True,
                **options
            }
            
            logger.info(f"[DEBUG] Reading {file_pattern} with delimiter '{preferred_delimiter}'")
            logger.info(f"[DEBUG] Options: {read_options}")
            
            df = self.platform.read_batch_files(
                batch_id,
                file_pattern,
                format="csv",
                **read_options
            )
            
            logger.info(f"[DEBUG] Read {file_pattern} with delimiter '{preferred_delimiter}': got {len(df.columns)} columns")
            
            # Check if we got expected columns
            if len(df.columns) >= expected_cols:
                logger.info(f"[DEBUG] Successfully read {file_pattern} with delimiter '{preferred_delimiter}' ({len(df.columns)} columns)")
                return df
            else:
                logger.warning(
                    f"[DEBUG] {file_pattern} read with '{preferred_delimiter}' but only got {len(df.columns)} columns "
                    f"(expected {expected_cols}). Sample data shows pipes are present. "
                    f"Trying with multiLine=false and different quote settings..."
                )
                
                # Try with multiLine=false (sometimes helps with delimiter recognition)
                read_options_fixed = {
                    "sep": preferred_delimiter,
                    "delimiter": preferred_delimiter,
                    "header": False,
                    "inferSchema": True,
                    "multiLine": False,
                    "quote": "",  # No quote character
                    "escape": "",  # No escape character
                    **options
                }
                df = self.platform.read_batch_files(
                    batch_id,
                    file_pattern,
                    format="csv",
                    **read_options_fixed
                )
                logger.info(f"[DEBUG] Read with multiLine=false: got {len(df.columns)} columns")
                
                if len(df.columns) >= expected_cols:
                    return df
                else:
                    logger.warning(
                        f"[DEBUG] multiLine=false attempt gave {len(df.columns)} columns "
                        f"(expected {expected_cols}), continuing to try other methods..."
                    )
        except Exception as e:
            logger.warning(f"[DEBUG] Failed to read {file_pattern} with delimiter '{preferred_delimiter}': {e}")
        
        # Try alternative delimiter
        alt_delimiter = "," if preferred_delimiter == "|" else "|"
        logger.info(f"[DEBUG] Trying alternative delimiter '{alt_delimiter}' for {file_pattern}")
        try:
            df = self.platform.read_batch_files(
                batch_id,
                file_pattern,
                format="csv",
                sep=alt_delimiter,
                delimiter=alt_delimiter,
                header=False,
                inferSchema=True,
                **options
            )
            
            logger.info(f"[DEBUG] Read with '{alt_delimiter}': got {len(df.columns)} columns")
            if len(df.columns) >= expected_cols:
                logger.info(f"[DEBUG] Successfully read {file_pattern} with delimiter '{alt_delimiter}' ({len(df.columns)} columns)")
                return df
            else:
                logger.warning(
                    f"[DEBUG] Alternative delimiter '{alt_delimiter}' gave only {len(df.columns)} columns "
                    f"(expected {expected_cols}), continuing to manual split..."
                )
        except Exception as e:
            logger.warning(f"[DEBUG] Failed with alternative delimiter '{alt_delimiter}': {e}")
        
        # If still failing, use the SQL-based split method
        logger.warning(
            f"[DEBUG] All delimiter attempts failed for {file_pattern}. "
            f"Using SQL-based split on pipe delimiter..."
        )
        
        return self._read_pipe_delimited_txt(batch_id, file_pattern, expected_cols)
    
    def _read_customermgmt_xml(self, batch_id: int) -> DataFrame:
        """
        Read CustomerMgmt.xml file and parse XML structure.
        
        TPC-DI XML structure:
        <TPCDI:Actions>
          <Action>
            <ActionType>NEW|UPD|INACT</ActionType>
            <Customer>...</Customer> or <Account>...</Account>
          </Action>
        </TPCDI:Actions>
        
        Returns DataFrame with flattened structure.
        
        Note: Requires spark-xml library (com.databricks:spark-xml_2.12:0.15.0)
        """
        logger.info(f"Reading CustomerMgmt.xml from Batch{batch_id}")
        
        try:
            file_path = f"Batch{batch_id}/CustomerMgmt.xml"
            # Use rowTag="Action" to parse each Action element as a row
            # The root tag TPCDI:Actions is handled automatically by spark-xml
            df = self.platform.read_raw_file(
                file_path,
                format="xml",
                rowTag="Action"
            )
            logger.info(f"Successfully read CustomerMgmt.xml: {len(df.columns)} columns")
            logger.info(f"Columns: {df.columns}")
            logger.info(f"[DEBUG] Sample XML structure:")
            df.show(3, truncate=100)
            
            # Debug: Show ActionType distribution
            try:
                if "ActionType" in df.columns:
                    action_types = df.groupBy("ActionType").count().orderBy("count", ascending=False)
                    logger.info(f"[DEBUG] ActionType distribution:")
                    action_types.show()
            except Exception as e:
                logger.debug(f"Could not show ActionType distribution: {e}")
            
            return df
        except Exception as e:
            error_msg = str(e)
            if "xml" in error_msg.lower() or "datasource" in error_msg.lower() or "format" in error_msg.lower():
                logger.error(
                    f"Failed to read CustomerMgmt.xml. This requires the spark-xml library.\n"
                    f"Install it on your cluster:\n"
                    f"  Maven: com.databricks:spark-xml_2.12:0.15.0\n"
                    f"  Or add to cluster libraries via Cluster -> Libraries -> Install New -> Maven\n"
                    f"Original error: {error_msg}"
                )
            else:
                logger.error(f"Failed to read CustomerMgmt.xml: {error_msg}")
            raise
    
    def _validate_and_debug_df(self, df: DataFrame, file_name: str, 
                               expected_cols: int, expected_format: str):
        """Validate DataFrame and log debug information."""
        logger.info(f"[DEBUG] Validating {file_name}:")
        logger.info(f"  Expected columns: {expected_cols}")
        logger.info(f"  Expected format: {expected_format}")
        logger.info(f"  Actual columns: {len(df.columns)}")
        logger.info(f"  Column names: {df.columns}")
        
        if len(df.columns) < expected_cols:
            logger.warning(
                f"{file_name} has {len(df.columns)} columns but expected at least {expected_cols}. "
                f"Actual columns: {df.columns}. Expected format: {expected_format}"
            )
        
        try:
            logger.info(f"[DEBUG] Sample rows from {file_name}:")
            df.show(5, truncate=50)
        except Exception as e:
            logger.warning(f"[DEBUG] Could not show sample rows: {e}")
    
    def load_dim_date(self, target_table: str) -> DataFrame:
        """
        Load Date dimension table from Batch1/Date.txt (TPC-DI spec: historical data in Batch1)
        
        TPC-DI format: Pipe-delimited (.txt file)
        Format: DateValue|DayOfWeek|CalendarMonth|CalendarQuarter|CalendarYear|DayOfMonth
        """
        logger.info("Loading DimDate dimension table")
        
        # Read Date.txt from Batch1 - pipe-delimited per TPC-DI spec
        df = self._read_file_with_delimiter_detection(
            1,  # Batch1 contains historical load data
            "Date.txt",
            expected_cols=6,
            expected_format="DateValue|DayOfWeek|CalendarMonth|CalendarQuarter|CalendarYear|DayOfMonth",
            preferred_delimiter="|"
        )
        
        # Validate and debug
        self._validate_and_debug_df(
            df, 
            "Date.txt", 
            expected_cols=6,
            expected_format="DateValue|DayOfWeek|CalendarMonth|CalendarQuarter|CalendarYear|DayOfMonth"
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
        """
        Load Time dimension table from Batch1/Time.txt (TPC-DI spec: historical data in Batch1)
        
        TPC-DI format: Pipe-delimited (.txt file)
        Format: Time|TimeID|Hour|Minute|Second|AM|PM
        """
        logger.info("Loading DimTime dimension table")
        
        # Read Time.txt from Batch1 - pipe-delimited per TPC-DI spec
        df = self._read_file_with_delimiter_detection(
            1,  # Batch1 contains historical load data
            "Time.txt",
            expected_cols=7,
            expected_format="Time|TimeID|Hour|Minute|Second|AM|PM",
            preferred_delimiter="|"
        )
        
        # Validate and debug
        self._validate_and_debug_df(
            df,
            "Time.txt",
            expected_cols=7,
            expected_format="Time|TimeID|Hour|Minute|Second|AM|PM"
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
        """Load TradeType dimension table from Batch1/TradeType.txt"""
        logger.info("Loading DimTradeType dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,
            "TradeType.txt",
            expected_cols=2,
            expected_format="TT_ID|TT_NAME",
            preferred_delimiter="|"
        )
        
        self._validate_and_debug_df(df, "TradeType.txt", expected_cols=2, expected_format="TT_ID|TT_NAME")
        
        dim_trade_type = df.select(
            col("_c0").alias("TT_ID"),
            col("_c1").alias("TT_NAME"),
            col("_c1").alias("TT_IS_SELL"),
            col("_c1").alias("TT_IS_MRKT"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_trade_type, target_table, mode="overwrite")
        return dim_trade_type
    
    def load_dim_status_type(self, target_table: str) -> DataFrame:
        """Load StatusType dimension table from Batch1/StatusType.txt"""
        logger.info("Loading DimStatusType dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,
            "StatusType.txt",
            expected_cols=2,
            expected_format="ST_ID|ST_NAME",
            preferred_delimiter="|"
        )
        
        self._validate_and_debug_df(df, "StatusType.txt", expected_cols=2, expected_format="ST_ID|ST_NAME")
        
        dim_status_type = df.select(
            col("_c0").alias("ST_ID"),
            col("_c1").alias("ST_NAME"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_status_type, target_table, mode="overwrite")
        return dim_status_type
    
    def load_dim_tax_rate(self, target_table: str) -> DataFrame:
        """Load TaxRate dimension table from Batch1/TaxRate.txt"""
        logger.info("Loading DimTaxRate dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,
            "TaxRate.txt",
            expected_cols=2,
            expected_format="TX_ID|TX_NAME",
            preferred_delimiter="|"
        )
        
        self._validate_and_debug_df(df, "TaxRate.txt", expected_cols=2, expected_format="TX_ID|TX_NAME")
        
        dim_tax_rate = df.select(
            col("_c0").alias("TX_ID"),
            col("_c1").alias("TX_NAME"),
            col("_c1").alias("TX_RATE"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_tax_rate, target_table, mode="overwrite")
        return dim_tax_rate
    
    def load_dim_industry(self, target_table: str) -> DataFrame:
        """Load Industry dimension table from Batch1/Industry.txt"""
        logger.info("Loading DimIndustry dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,
            "Industry.txt",
            expected_cols=4,
            expected_format="IN_ID|IN_NAME|IN_SC_ID|IN_SC_NAME",
            preferred_delimiter="|"
        )
        
        self._validate_and_debug_df(df, "Industry.txt", expected_cols=4, expected_format="IN_ID|IN_NAME|IN_SC_ID|IN_SC_NAME")
        
        dim_industry = df.select(
            col("_c0").alias("IN_ID"),
            col("_c1").alias("IN_NAME"),
            col("_c2").alias("IN_SC_ID"),
            col("_c3").alias("IN_SC_NAME"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_industry, target_table, mode="overwrite")
        return dim_industry
    
    def load_dim_account(self, target_table: str) -> DataFrame:
        """
        Load Account dimension table from Batch1/CustomerMgmt.xml
        
        TPC-DI XML structure:
        <TPCDI:Actions>
          <Action ActionType="NEW" ActionTS="2024-05-20T12:00:00">
            <Customer C_ID="123">
              <Name>...</Name>
              <Account CA_ID="987" CA_TAX_ST="1">
                ...
              </Account>
            </Customer>
          </Action>
        </TPCDI:Actions>
        
        Note: CustomerMgmt.xml is ALWAYS XML format per TPC-DI spec. There is no .txt version.
        Account data is nested inside Customer elements.
        """
        logger.info("Loading DimAccount dimension table from CustomerMgmt.xml")
        
        # Read XML file (per TPC-DI spec, CustomerMgmt is always XML)
        xml_df = self._read_customermgmt_xml(1)
        
        # Show schema to understand the structure
        logger.info("[DEBUG] XML DataFrame schema:")
        xml_df.printSchema()
        logger.info("[DEBUG] Sample XML rows:")
        xml_df.show(2, truncate=200, vertical=True)
        
        # Extract Account data from XML using the correct SQL pattern
        # Structure: Action -> Customer -> Account
        # Attributes use underscore prefix: _ActionType, _ActionTS, _C_ID, _CA_ID, _CA_TAX_ST
        # Elements don't use underscore: CA_NAME
        logger.info("[DEBUG] Extracting Account data from XML using SQL pattern...")
        
        # Use the exact SQL pattern provided:
        # SELECT
        #   _ActionType as ActionType,
        #   _ActionTS as ActionTS,
        #   Customer._C_ID as CustomerID,
        #   Customer.Account._CA_ID as AccountID,
        #   Customer.Account._CA_TAX_ST as TaxStatus,
        #   Customer.Account.CA_NAME as AccountName
        # FROM customer_mgmt_raw_xml
        
        try:
            account_df = xml_df.select(
                col("_ActionType").alias("ActionType"),
                col("_ActionTS").alias("ActionTS"),
                col("Customer._C_ID").alias("CustomerID"),
                col("Customer.Account._CA_ID").alias("AccountID"),
                col("Customer.Account._CA_TAX_ST").alias("TaxStatus"),
                col("Customer.Account.CA_NAME").alias("AccountName")
            ).filter(col("Customer.Account._CA_ID").isNotNull())
            
            logger.info(f"[DEBUG] Successfully extracted {account_df.count()} accounts from XML")
        except Exception as e:
            logger.error(f"[DEBUG] Failed to extract Account data: {e}")
            logger.info("[DEBUG] XML DataFrame schema for debugging:")
            xml_df.printSchema()
            logger.info("[DEBUG] Sample XML rows:")
            xml_df.show(2, truncate=200, vertical=True)
            raise RuntimeError(
                f"Failed to extract Account data from CustomerMgmt.xml.\n"
                f"Error: {e}\n\n"
                f"Expected structure:\n"
                f"  - _ActionType (attribute on Action)\n"
                f"  - _ActionTS (attribute on Action)\n"
                f"  - Customer._C_ID (attribute on Customer)\n"
                f"  - Customer.Account._CA_ID (attribute on Account)\n"
                f"  - Customer.Account._CA_TAX_ST (attribute on Account)\n"
                f"  - Customer.Account.CA_NAME (element on Account)\n\n"
                f"Please check:\n"
                f"1. spark-xml library is installed (com.databricks:spark-xml_2.12:0.15.0)\n"
                f"2. XML file structure matches TPC-DI spec\n"
                f"3. Check the schema output above to see actual structure"
            ) from e
        
        # Show extracted data
        logger.info(f"[DEBUG] Extracted Account data:")
        account_df.show(5, truncate=50)
        logger.info(f"[DEBUG] Extracted Account schema:")
        account_df.printSchema()
        
        # Transform to DimAccount schema
        # Map TPC-DI XML fields to DimAccount columns:
        # AccountID (from Customer.Account._CA_ID) -> SK_AccountID
        # CustomerID (from Customer._C_ID) -> SK_CustomerID
        # TaxStatus (from Customer.Account._CA_TAX_ST) -> TaxStatus
        # AccountName (from Customer.Account.CA_NAME) -> AccountDesc
        # ActionType (from _ActionType) -> Status
        # ActionType == "INACT" -> IsActive = False
        dim_account = account_df.select(
            col("AccountID").alias("SK_AccountID"),
            lit(None).cast("bigint").alias("SK_BrokerID"),  # May need to extract from elsewhere in XML
            col("CustomerID").alias("SK_CustomerID"),
            col("ActionType").alias("Status"),
            col("AccountName").alias("AccountDesc"),
            col("TaxStatus"),
            when(col("ActionType") == "INACT", lit(False)).otherwise(lit(True)).alias("IsActive"),
            current_timestamp().alias("BatchID")
        )
        
        logger.info(f"[DEBUG] Final DimAccount schema:")
        dim_account.printSchema()
        logger.info(f"[DEBUG] Final DimAccount row count: {dim_account.count()}")
        dim_account.show(5, truncate=50)
        
        self.platform.write_table(dim_account, target_table, mode="overwrite")
        return dim_account
    
    def run_full_batch_load(self, target_database: str, target_schema: str):
        """
        Run the full batch load process (historical load from Batch1).
        
        Args:
            target_database: Target database/catalog name
            target_schema: Target schema name
        """
        logger.info("Starting full batch load process")
        
        # Load dimension tables
        self.load_dim_date(f"{target_database}.{target_schema}.DimDate")
        self.load_dim_time(f"{target_database}.{target_schema}.DimTime")
        self.load_dim_trade_type(f"{target_database}.{target_schema}.DimTradeType")
        self.load_dim_status_type(f"{target_database}.{target_schema}.DimStatusType")
        self.load_dim_tax_rate(f"{target_database}.{target_schema}.DimTaxRate")
        self.load_dim_industry(f"{target_database}.{target_schema}.DimIndustry")
        self.load_dim_account(f"{target_database}.{target_schema}.DimAccount")
        
        logger.info("Full batch load completed")
