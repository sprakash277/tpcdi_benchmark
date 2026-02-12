"""
Bronze layer loader for Customer.txt (incremental batches only).

Ingests raw pipe-delimited customer data from Customer.txt.
Only present in Batch 2+ (incremental loads).
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeCustomer(BronzeLoaderBase):
    """
    Bronze layer loader for Customer.txt.
    
    Customer.txt is pipe-delimited customer data (state-at-time snapshot).
    Only present in Batch 2+ (incremental loads).
    
    Batch 1 uses CustomerMgmt.xml instead.
    
    At Bronze layer, we store each line as a raw string.
    """
    
    def load(self, batch_id: int, target_table: str) -> Optional[DataFrame]:
        """
        Ingest Customer.txt as raw pipe-delimited data.
        
        Args:
            batch_id: Batch number (should be 2+)
            target_table: Full target table name
            
        Returns:
            DataFrame with raw_line column, or None if file not found
        """
        logger.info(f"Loading bronze_customer from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/Customer.txt"
        
        try:
            # Read as text (raw lines)
            df = self.platform.read_raw_file(file_path, format="text")
            bronze_df = df.withColumnRenamed("value", "raw_line")
            
            return self._write_bronze_table(bronze_df, target_table, batch_id, "Customer.txt")
            
        except Exception as e:
            logger.warning(f"Customer.txt not found for Batch{batch_id}: {e}")
            return None
