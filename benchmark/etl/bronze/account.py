"""
Bronze layer loader for Account.txt (incremental batches only).

Ingests raw pipe-delimited account data from Account.txt.
Only present in Batch 2+ (incremental loads).
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

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
        
        try:
            # Read as text (raw lines)
            df = self.platform.read_raw_file(file_path, format="text")
            bronze_df = df.withColumnRenamed("value", "raw_line")
            
            return self._write_bronze_table(bronze_df, target_table, batch_id, "Account.txt")
            
        except Exception as e:
            logger.warning(f"Account.txt not found for Batch{batch_id}: {e}")
            return None
