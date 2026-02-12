"""
Bronze layer loader for FINWIRE files.

Ingests raw fixed-width strings from FINWIRE files without parsing.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeFinwire(BronzeLoaderBase):
    """
    Bronze layer loader for FINWIRE files.
    
    FINWIRE files are fixed-width and contain mixed record types:
    - CMP (Company): positions 16-18 = 'CMP'
    - SEC (Security): positions 16-18 = 'SEC'
    - FIN (Financial): positions 16-18 = 'FIN'
    
    At Bronze layer, we store each line as a raw string without parsing.
    """
    
    def load(self, batch_id: int, target_table: str) -> Optional[DataFrame]:
        """
        Ingest FINWIRE files as raw fixed-width strings.
        
        Args:
            batch_id: Batch number (typically 1 for historical)
            target_table: Full target table name
            
        Returns:
            DataFrame with raw_line column, or None if no files found
        """
        logger.info(f"Loading bronze_finwire from Batch{batch_id}")
        
        # FINWIRE files are named FINWIRE<year>Q<quarter>, e.g. FINWIRE1967Q1
        file_pattern = f"Batch{batch_id}/FINWIRE*"
        
        try:
            # Read as text (each line = one row with column 'value')
            df = self.platform.read_raw_file(file_pattern, format="text")
            
            # Rename to raw_line for clarity
            bronze_df = df.withColumnRenamed("value", "raw_line")
            
            return self._write_bronze_table(bronze_df, target_table, batch_id, "FINWIRE*")
            
        except Exception as e:
            logger.warning(f"No FINWIRE files found for Batch{batch_id}: {e}")
            return None
