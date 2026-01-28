"""
Batch ETL transformations for TPC-DI benchmark.
Handles historical load and initial batch processing.
"""

import logging
from typing import TYPE_CHECKING
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, upper, regexp_replace, lit, current_timestamp, split, element_at

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
        
        # Try preferred delimiter first
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
        
        # If still failing, try reading as text and manually splitting
        logger.warning(
            f"[DEBUG] All delimiter attempts failed for {file_pattern}. "
            f"Trying to read as text and manually split on pipe delimiter..."
        )
        
        # Read as text (single column with full line)
        text_df = self.platform.read_batch_files(
            batch_id,
            file_pattern,
            format="text"
        )
        
        # Manually split on pipe delimiter
        # text format returns a column named 'value'
        # Use regex to split on pipe character
        split_cols = split(text_df["value"], "\\|", -1)  # -1 to include trailing empty strings
        
        # Create individual columns based on expected count
        # Use element_at() function which is more reliable than getItem()
        # element_at() uses 1-based indexing, so we add 1 to the index
        select_exprs = []
        for i in range(expected_cols):
            # element_at(array, index) - uses 1-based indexing
            array_element = element_at(split_cols, i + 1)
            select_exprs.append(trim(array_element).alias(f"_c{i}"))
        
        # Create the DataFrame with split columns
        df = text_df.select(*select_exprs)
        
        logger.info(f"[DEBUG] Manually split {file_pattern} on pipe delimiter: got {len(df.columns)} columns")
        return df
    
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
            # Read XML file using Spark XML format
            # Spark XML format requires rowTag option to specify the repeating element
            df = self.platform.read_batch_files(
                batch_id,
                "CustomerMgmt.xml",
                format="xml",
                rowTag="Action",  # Each <Action> becomes a row
                excludeAttribute=False,  # Include XML attributes
                treatEmptyValuesAsNulls=True,
            )
            
            logger.info(f"[DEBUG] CustomerMgmt.xml read successfully: {len(df.columns)} columns")
            logger.info(f"[DEBUG] Columns: {df.columns}")
            logger.info(f"[DEBUG] Sample rows (first 2):")
            df.show(2, truncate=False)
            
            return df
        except Exception as e:
            error_msg = str(e)
            if "xml" in error_msg.lower() or "format" in error_msg.lower():
                raise RuntimeError(
                    f"Failed to read XML format. "
                    f"Spark XML format requires 'spark-xml' library. "
                    f"Install it in your cluster: com.databricks:spark-xml_2.12:0.15.0 "
                    f"Original error: {error_msg}"
                ) from e
            raise
    
    def _validate_and_debug_df(self, df: DataFrame, file_name: str, expected_cols: int, expected_format: str):
        """
        Validate DataFrame has expected columns and print debug info.
        
        Args:
            df: DataFrame to validate
            file_name: Name of the file being read (for error messages)
            expected_cols: Minimum expected number of columns
            expected_format: Expected format description
        """
        logger.info(f"[DEBUG] Reading {file_name}:")
        logger.info(f"  Schema: {df.schema}")
        logger.info(f"  Column count: {len(df.columns)}")
        logger.info(f"  Columns: {df.columns}")
        logger.info(f"  Sample rows (first 3):")
        df.show(3, truncate=False)
        
        num_cols = len(df.columns)
        if num_cols < expected_cols:
            raise ValueError(
                f"{file_name} has {num_cols} columns but expected at least {expected_cols}. "
                f"Actual columns: {df.columns}. "
                f"Expected format: {expected_format}. "
                f"Note: TPC-DI files should be pipe-delimited (|) for .txt files, "
                f"comma-delimited (,) for .csv files (HR.csv, Prospect.csv)."
            )
    
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
        """
        Load TradeType dimension from Batch1/TradeType.txt (TPC-DI spec: historical data in Batch1)
        
        TPC-DI format: Pipe-delimited (.txt file)
        Format: TT_ID|TT_NAME|TT_IS_SELL|TT_IS_MRKT
        """
        logger.info("Loading DimTradeType dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,  # Batch1 contains historical load data
            "TradeType.txt",
            expected_cols=4,
            expected_format="TT_ID|TT_NAME|TT_IS_SELL|TT_IS_MRKT",
            preferred_delimiter="|"
        )
        
        # Validate and debug
        self._validate_and_debug_df(
            df,
            "TradeType.txt",
            expected_cols=4,
            expected_format="TT_ID|TT_NAME|TT_IS_SELL|TT_IS_MRKT"
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
        """
        Load StatusType dimension from Batch1/StatusType.txt (TPC-DI spec: historical data in Batch1)
        
        TPC-DI format: Pipe-delimited (.txt file)
        Format: ST_ID|ST_NAME
        """
        logger.info("Loading DimStatusType dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,  # Batch1 contains historical load data
            "StatusType.txt",
            expected_cols=2,
            expected_format="ST_ID|ST_NAME",
            preferred_delimiter="|"
        )
        
        # Validate and debug
        self._validate_and_debug_df(
            df,
            "StatusType.txt",
            expected_cols=2,
            expected_format="ST_ID|ST_NAME"
        )
        
        dim_status_type = df.select(
            col("_c0").alias("ST_ID"),
            col("_c1").alias("ST_NAME"),
            current_timestamp().alias("BatchID")
        )
        
        self.platform.write_table(dim_status_type, target_table, mode="overwrite")
        return dim_status_type
    
    def load_dim_tax_rate(self, target_table: str) -> DataFrame:
        """
        Load TaxRate dimension from Batch1/TaxRate.txt (TPC-DI spec: historical data in Batch1)
        
        TPC-DI format: Pipe-delimited (.txt file)
        Format: TX_ID|TX_NAME|TX_RATE
        """
        logger.info("Loading DimTaxRate dimension table")
        
        df = self._read_file_with_delimiter_detection(
            1,  # Batch1 contains historical load data
            "TaxRate.txt",
            expected_cols=3,
            expected_format="TX_ID|TX_NAME|TX_RATE",
            preferred_delimiter="|"
        )
        
        # Validate and debug
        self._validate_and_debug_df(
            df,
            "TaxRate.txt",
            expected_cols=3,
            expected_format="TX_ID|TX_NAME|TX_RATE"
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
        """
        Load Industry dimension from Batch1/Industry.txt (TPC-DI spec: historical data in Batch1)
        
        TPC-DI format: Pipe-delimited (.txt file)
        Format: IN_ID|IN_NAME|IN_SC_ID|IN_SC_NAME
        
        Note: If file appears to be fixed-width or single-column, check the raw file content
        in the debug logs to determine the actual format.
        """
        logger.info("Loading DimIndustry dimension table")
        
        try:
            df = self._read_file_with_delimiter_detection(
                1,  # Batch1 contains historical load data
                "Industry.txt",
                expected_cols=4,
                expected_format="IN_ID|IN_NAME|IN_SC_ID|IN_SC_NAME",
                preferred_delimiter="|"
            )
            
            # Validate and debug
            self._validate_and_debug_df(
                df,
                "Industry.txt",
                expected_cols=4,
                expected_format="IN_ID|IN_NAME|IN_SC_ID|IN_SC_NAME"
            )
        except ValueError as e:
            # If validation fails, provide more helpful error
            logger.error(f"Failed to parse Industry.txt: {e}")
            logger.error(
                "Possible causes:\n"
                "1. File might be fixed-width format (no delimiters)\n"
                "2. File might use a different delimiter\n"
                "3. File structure might differ from TPC-DI spec\n"
                "Check the debug logs above for raw file content."
            )
            raise
        
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
        """
        Load Account dimension from Batch files.
        
        TPC-DI spec: CustomerMgmt.xml is XML format with nested structure:
        - Root: <TPCDI:Actions>
        - Elements: <Action> with ActionType (NEW, UPD, INACT, etc.)
        - Contains: <Customer> or <Account> blocks
        
        Some implementations may provide CustomerMgmt.txt as pipe-delimited.
        This method tries XML first, then falls back to pipe-delimited text.
        """
        logger.info(f"Loading DimAccount dimension table from batch {batch_id}")
        
        # Try CustomerMgmt.xml first (XML format per TPC-DI spec)
        xml_error = None
        try:
            logger.info("Attempting to read CustomerMgmt.xml (XML format)")
            df = self._read_customermgmt_xml(batch_id)
            logger.info("Successfully read CustomerMgmt.xml")
        except Exception as e:
            xml_error = e
            logger.warning(f"Failed to read CustomerMgmt.xml: {e}")
            # Check if it's a path not found error - might be .txt file instead
            if "PATH_NOT_FOUND" in str(e) or "does not exist" in str(e).lower():
                logger.info("CustomerMgmt.xml not found, will try CustomerMgmt.txt")
            else:
                logger.info("CustomerMgmt.xml exists but failed to parse, will try CustomerMgmt.txt")
        
        # Fallback to CustomerMgmt.txt (pipe-delimited) if XML failed
        if xml_error:
            logger.info("Falling back to CustomerMgmt.txt (pipe-delimited)")
            try:
                df = self._read_file_with_delimiter_detection(
                    batch_id,
                    "CustomerMgmt.txt",
                    expected_cols=8,
                    expected_format="ActionType|AccountID|... (pipe-delimited)",
                    preferred_delimiter="|"
                )
                logger.info("Successfully read CustomerMgmt.txt")
            except Exception as txt_error:
                logger.error(f"Failed to read CustomerMgmt.txt: {txt_error}")
                raise ValueError(
                    f"Could not read CustomerMgmt file from Batch{batch_id}. "
                    f"Tried CustomerMgmt.xml (XML format): {xml_error}. "
                    f"Tried CustomerMgmt.txt (pipe-delimited): {txt_error}. "
                    f"Please ensure one of these files exists in Batch{batch_id}/ directory."
                ) from txt_error
        
        # Check if this is XML format (has ActionType column) or text format (has _c0)
        if "ActionType" in df.columns:
            # XML format: filter for Account actions and extract Account data
            logger.info("Processing CustomerMgmt.xml (XML format)")
            logger.info(f"[DEBUG] XML DataFrame columns: {df.columns}")
            logger.info(f"[DEBUG] XML DataFrame schema: {df.schema}")
            df.show(2, truncate=False)
            
            # Filter for actions that have Account data (not Customer data)
            # In TPC-DI XML, each Action can have either Customer or Account
            account_actions = df.filter(col("Account").isNotNull())
            
            logger.info(f"[DEBUG] Found {account_actions.count()} Account actions")
            
            # Extract Account fields from nested XML structure
            # XML structure: <Action><Account><field>value</field></Account></Action>
            # Spark XML reader flattens this structure
            # Field names depend on actual XML structure - may need adjustment
            try:
                dim_account = account_actions.select(
                    # Try common XML field access patterns
                    col("Account.AccountID").alias("AccountID"),
                    col("Account.SK_BrokerID").alias("SK_BrokerID"),
                    col("Account.SK_CustomerID").alias("SK_CustomerID"),
                    col("Account.Status").alias("Status"),
                    col("Account.AccountDesc").alias("AccountDesc"),
                    col("Account.TaxStatus").alias("TaxStatus"),
                    col("Account.IsCurrent").alias("IsCurrent"),
                    col("ActionType"),
                    lit(batch_id).alias("BatchID"),
                    current_timestamp().alias("EffectiveDate"),
                    current_timestamp().alias("EndDate")
                )
            except Exception as e:
                # If field access fails, try alternative patterns
                logger.warning(f"Failed to access Account fields with standard pattern: {e}")
                logger.info("Trying alternative XML field access patterns...")
                # Try with underscore prefix (for attributes)
                try:
                    dim_account = account_actions.select(
                        col("Account._AccountID").alias("AccountID"),
                        col("Account._SK_BrokerID").alias("SK_BrokerID"),
                        col("Account._SK_CustomerID").alias("SK_CustomerID"),
                        col("Account._Status").alias("Status"),
                        col("Account._AccountDesc").alias("AccountDesc"),
                        col("Account._TaxStatus").alias("TaxStatus"),
                        col("Account._IsCurrent").alias("IsCurrent"),
                        col("ActionType"),
                        lit(batch_id).alias("BatchID"),
                        current_timestamp().alias("EffectiveDate"),
                        current_timestamp().alias("EndDate")
                    )
                except Exception as e2:
                    logger.error(f"Failed with underscore pattern: {e2}")
                    logger.error("Please check the actual XML structure. Available columns:")
                    account_actions.select("Account.*").show(1, truncate=False)
                    raise ValueError(
                        f"Could not extract Account fields from XML. "
                        f"Please check the XML structure. Error: {e2}"
                    ) from e2
        else:
            # Text format: filter for Account records (record type 'A')
            logger.info("Processing CustomerMgmt.txt (pipe-delimited format)")
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
        
        # Database/schema created by runner; load dimension tables (historical)
        self.load_dim_date(f"{target_database}.{target_schema}.DimDate")
        self.load_dim_time(f"{target_database}.{target_schema}.DimTime")
        self.load_dim_trade_type(f"{target_database}.{target_schema}.DimTradeType")
        self.load_dim_status_type(f"{target_database}.{target_schema}.DimStatusType")
        self.load_dim_tax_rate(f"{target_database}.{target_schema}.DimTaxRate")
        self.load_dim_industry(f"{target_database}.{target_schema}.DimIndustry")
        
        # Load fact and other dimension tables from Batch1
        self.load_dim_account(f"{target_database}.{target_schema}.DimAccount", batch_id=1)
        
        logger.info("Completed full batch load process")
