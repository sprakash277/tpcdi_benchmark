"""
Bronze layer loader for Account.txt (incremental batches only).

Ingests raw pipe-delimited account data from Account.txt.
Only present in Batch 2+ (incremental loads).
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase, ensure_bronze_table_exists

logger = logging.getLogger(__name__)


class BronzeAccount(BronzeLoaderBase):
    """
    Bronze layer loader for Account.txt.
    
    Account.txt is pipe-delimited account data (state-at-time snapshot).
    Only present in Batch 2+ (incremental loads).
    
    Batch 1 uses CustomerMgmt.xml instead.
    
    At Bronze layer, we store each line as a raw string.
    """
    
    def load(self, batch_id: int, target_table: str) -> Optional[DataFrame]:
        """
        Ingest Account.txt as raw pipe-delimited data.
        
        Args:
            batch_id: Batch number (should be 2+)
            target_table: Full target table name
            
        Returns:
            DataFrame with raw_line column, or None if file not found
        """
        logger.info(f"Loading bronze_account from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/Account.txt"
        
        bronze_df = None
        try:
            # Read as text (raw lines)
            df = self.platform.read_raw_file(file_path, format="text")
            bronze_df = df.withColumnRenamed("value", "raw_line")
            # Incremental: table may not exist yet; create empty so append succeeds (Delta)
            ensure_bronze_table_exists(self.spark, self.platform, target_table)
            return self._write_bronze_table(bronze_df, target_table, batch_id, "Account.txt")
        except Exception as e:
            # Retry once if append failed because table did not exist (e.g. ensure_bronze_table_exists not run or failed)
            if bronze_df is not None and "DELTA_TABLE_NOT_FOUND" in str(e):
                try:
                    logger.info("Creating bronze_account table and retrying write after DELTA_TABLE_NOT_FOUND")
                    ensure_bronze_table_exists(self.spark, self.platform, target_table)
                    return self._write_bronze_table(bronze_df, target_table, batch_id, "Account.txt")
                except Exception as e2:
                    logger.warning(f"Account.txt / bronze_account write failed after retry: {e2}")
                    return None
            logger.warning(f"Account.txt not found for Batch{batch_id}: {e}")
            return None
