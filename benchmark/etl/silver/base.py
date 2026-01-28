"""
Base class for Silver layer ETL loaders.

Provides common functionality for data cleaning and transformation,
including CDC (Change Data Capture) and SCD Type 2 handling.
"""

import logging
from typing import TYPE_CHECKING, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, coalesce

if TYPE_CHECKING:
    from benchmark.platforms.databricks import DatabricksPlatform
    from benchmark.platforms.dataproc import DataprocPlatform

logger = logging.getLogger(__name__)


class SilverLoaderBase:
    """
    Base class for Silver layer data loaders.
    
    Provides common functionality:
    - Platform adapter access
    - Pipe-delimited parsing
    - CSV parsing
    - Standardized write operations
    """
    
    def __init__(self, platform):
        """
        Initialize Silver loader.
        
        Args:
            platform: Platform adapter (DatabricksPlatform or DataprocPlatform)
        """
        self.platform = platform
        self.spark = platform.get_spark()
    
    def _extract_record_type(self, parsed_df: DataFrame, batch_id: int, col_offset: int = 0) -> tuple:
        """
        Extract record_type from first column for incremental loads.
        
        Args:
            parsed_df: Parsed DataFrame with _c0, _c1, etc. columns
            batch_id: Batch number
            col_offset: Offset to apply to column indices (default 0)
            
        Returns:
            Tuple of (record_type_column, actual_col_offset)
            - record_type_column: Column expression for record_type
            - actual_col_offset: Column offset to use for data columns (0 or 1)
        """
        def col_exists(idx: int) -> bool:
            return f"_c{idx}" in parsed_df.columns
        
        if batch_id > 1 and col_exists(col_offset):
            # Incremental load: extract record_type from first column
            return (col(f"_c{col_offset}").alias("record_type"), col_offset + 1)
        else:
            # Batch 1: no record_type, use default
            return (lit("I").alias("record_type"), col_offset)
    
    def _parse_pipe_delimited(self, df: DataFrame, num_cols: int) -> DataFrame:
        """
        Parse pipe-delimited raw_line into columns _c0, _c1, etc.
        
        Args:
            df: DataFrame with 'raw_line' column
            num_cols: Number of expected columns
            
        Returns:
            DataFrame with parsed columns
        """
        temp_view = f"_temp_pipe_parse_{id(df)}"
        df.createOrReplaceTempView(temp_view)
        
        select_parts = []
        for i in range(num_cols):
            select_parts.append(
                f"TRIM(COALESCE(element_at(split(raw_line, '\\\\|'), {i+1}), '')) AS _c{i}"
            )
        
        # Keep metadata columns
        select_parts.extend([
            "_load_timestamp",
            "_source_file", 
            "_batch_id"
        ])
        
        sql = f"SELECT {', '.join(select_parts)} FROM {temp_view}"
        
        try:
            result = self.spark.sql(sql)
        except Exception:
            # Retry without escape if needed
            select_parts = []
            for i in range(num_cols):
                select_parts.append(
                    f"TRIM(COALESCE(element_at(split(raw_line, '|'), {i+1}), '')) AS _c{i}"
                )
            select_parts.extend(["_load_timestamp", "_source_file", "_batch_id"])
            sql = f"SELECT {', '.join(select_parts)} FROM {temp_view}"
            result = self.spark.sql(sql)
        finally:
            try:
                self.spark.catalog.dropTempView(temp_view)
            except:
                pass
        
        return result
    
    def _parse_csv_delimited(self, df: DataFrame, num_cols: int) -> DataFrame:
        """
        Parse comma-delimited raw_line into columns _c0, _c1, etc.
        
        Args:
            df: DataFrame with 'raw_line' column
            num_cols: Number of expected columns
            
        Returns:
            DataFrame with parsed columns
        """
        temp_view = f"_temp_csv_parse_{id(df)}"
        df.createOrReplaceTempView(temp_view)
        
        select_parts = []
        for i in range(num_cols):
            select_parts.append(
                f"TRIM(COALESCE(element_at(split(raw_line, ','), {i+1}), '')) AS _c{i}"
            )
        select_parts.extend(["_load_timestamp", "_source_file", "_batch_id"])
        
        sql = f"SELECT {', '.join(select_parts)} FROM {temp_view}"
        result = self.spark.sql(sql)
        
        try:
            self.spark.catalog.dropTempView(temp_view)
        except:
            pass
        
        return result
    
    def _write_silver_table(self, df: DataFrame, target_table: str, 
                            batch_id: int) -> DataFrame:
        """
        Write DataFrame to Silver table.
        
        Args:
            df: Input DataFrame
            target_table: Full table name (catalog.schema.table)
            batch_id: Batch number
            
        Returns:
            DataFrame that was written
        """
        # Batch 1 = overwrite, subsequent batches = append
        mode = "overwrite" if batch_id == 1 else "append"
        self.platform.write_table(df, target_table, mode=mode)
        
        logger.info(f"Loaded {target_table}: {df.count()} rows (mode={mode})")
        return df
    
    def _apply_scd_type2(self, incoming_df: DataFrame, target_table: str,
                         key_column: str, effective_date_col: str = "effective_date") -> DataFrame:
        """
        Apply SCD Type 2 logic for incremental loads.
        
        This method:
        1. Closes out existing current records that have updates (set is_current=False, end_date)
        2. Inserts new versions of updated records
        3. Inserts completely new records
        
        Args:
            incoming_df: DataFrame with incoming changes
            target_table: Target table name
            key_column: Business key column (e.g., 'customer_id', 'account_id')
            effective_date_col: Column containing the effective date
            
        Returns:
            DataFrame with changes applied
        """
        logger.info(f"Applying SCD Type 2 to {target_table} on key {key_column}")
        
        # Check if target table exists
        try:
            existing_df = self.spark.table(target_table)
            table_exists = True
        except Exception:
            logger.info(f"Table {target_table} does not exist, will create")
            table_exists = False
        
        if not table_exists:
            # First load - just write directly
            self.platform.write_table(incoming_df, target_table, mode="overwrite")
            logger.info(f"Created {target_table} with {incoming_df.count()} rows")
            return incoming_df
        
        # Align schema: Read existing table and ensure incoming schema matches
        # This handles cases where Batch 1 had different types (e.g., dob as string vs date)
        try:
            existing_schema = existing_df.schema
            incoming_schema = incoming_df.schema
            
            # Check for schema mismatches and align incoming data
            schema_fields = {}
            for field in existing_schema:
                schema_fields[field.name] = field.dataType
            
            # Build select with type casts to match existing schema
            select_cols = []
            for field in incoming_schema:
                col_name = field.name
                if col_name in schema_fields:
                    existing_type = schema_fields[col_name]
                    incoming_type = field.dataType
                    if str(existing_type) != str(incoming_type):
                        # Type mismatch - cast to match existing
                        logger.info(f"Aligning column {col_name}: {incoming_type} -> {existing_type}")
                        select_cols.append(col(col_name).cast(existing_type).alias(col_name))
                    else:
                        select_cols.append(col(col_name))
                else:
                    # New column - keep as is
                    select_cols.append(col(col_name))
            
            if select_cols:
                incoming_df = incoming_df.select(*select_cols)
                logger.info(f"Schema aligned for {target_table}")
        except Exception as e:
            logger.warning(f"Schema alignment failed: {e}. Proceeding with original schema.")
        
        # Get business keys from incoming data
        incoming_keys = incoming_df.select(key_column).distinct()
        
        # Create temp views for SQL
        incoming_df.createOrReplaceTempView("incoming_changes")
        incoming_keys.createOrReplaceTempView("incoming_keys")
        
        # Step 1: Close out existing current records that have updates
        # Update is_current=False, end_date=incoming.effective_date for matching keys
        close_sql = f"""
        MERGE INTO {target_table} AS target
        USING (
            SELECT {key_column}, MIN({effective_date_col}) as new_effective_date
            FROM incoming_changes
            GROUP BY {key_column}
        ) AS updates
        ON target.{key_column} = updates.{key_column} AND target.is_current = true
        WHEN MATCHED THEN UPDATE SET
            target.is_current = false,
            target.end_date = updates.new_effective_date
        """
        
        try:
            self.spark.sql(close_sql)
            logger.info(f"Closed out existing records for updated keys in {target_table}")
        except Exception as e:
            logger.warning(f"MERGE failed (may not be Delta): {e}. Falling back to append-only.")
            # Fallback: just append (works for non-Delta tables)
            self.platform.write_table(incoming_df, target_table, mode="append")
            return incoming_df
        
        # Step 2: Insert new versions
        # Use mergeSchema=true for Delta append to handle schema evolution
        self.platform.write_table(incoming_df, target_table, mode="append")
        
        # Cleanup temp views
        try:
            self.spark.catalog.dropTempView("incoming_changes")
            self.spark.catalog.dropTempView("incoming_keys")
        except:
            pass
        
        logger.info(f"SCD Type 2 applied to {target_table}: {incoming_df.count()} new versions inserted")
        return incoming_df
    
    def _upsert_fact_table(self, incoming_df: DataFrame, target_table: str,
                           key_column: str, update_columns: Optional[List[str]] = None) -> DataFrame:
        """
        Upsert fact table data using MERGE.
        
        For CDC on fact tables:
        - If record exists (by key), update specified columns
        - If record doesn't exist, insert new record
        
        Args:
            incoming_df: DataFrame with incoming data
            target_table: Target table name
            key_column: Primary key column (e.g., 'trade_id')
            update_columns: Columns to update on match (None = update all)
            
        Returns:
            DataFrame with changes applied
        """
        logger.info(f"Upserting {target_table} on key {key_column}")
        
        # Check if target table exists
        try:
            existing_df = self.spark.table(target_table)
            table_exists = True
        except Exception:
            logger.info(f"Table {target_table} does not exist, will create")
            table_exists = False
        
        if not table_exists:
            # First load - just write directly
            self.platform.write_table(incoming_df, target_table, mode="overwrite")
            logger.info(f"Created {target_table} with {incoming_df.count()} rows")
            return incoming_df
        
        # Create temp view for incoming data
        incoming_df.createOrReplaceTempView("incoming_data")
        
        # Build UPDATE SET clause
        if update_columns is None:
            # Update all columns except the key
            update_columns = [c for c in incoming_df.columns if c != key_column]
        
        update_set = ", ".join([f"target.{c} = source.{c}" for c in update_columns])
        
        # Build INSERT columns
        all_columns = incoming_df.columns
        insert_cols = ", ".join(all_columns)
        insert_vals = ", ".join([f"source.{c}" for c in all_columns])
        
        # MERGE statement
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING incoming_data AS source
        ON target.{key_column} = source.{key_column}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        
        try:
            self.spark.sql(merge_sql)
            logger.info(f"MERGE completed for {target_table}")
        except Exception as e:
            logger.warning(f"MERGE failed (may not be Delta): {e}. Falling back to append-only.")
            # Fallback: just append (works for non-Delta tables)
            self.platform.write_table(incoming_df, target_table, mode="append")
        
        # Cleanup temp view
        try:
            self.spark.catalog.dropTempView("incoming_data")
        except:
            pass
        
        return incoming_df
