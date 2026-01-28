"""
Bronze layer loader for CustomerMgmt.xml.

Ingests raw XML structure from CustomerMgmt.xml with no field extraction.
"""

import logging
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeCustomerMgmt(BronzeLoaderBase):
    """
    Bronze layer loader for CustomerMgmt.xml.
    
    The XML is read with spark-xml and stored as nested struct.
    No field extraction - just raw capture of the XML structure.
    """
    
    def load(self, batch_id: int, target_table: str) -> DataFrame:
        """
        Ingest CustomerMgmt.xml as raw XML structure.
        
        Args:
            batch_id: Batch number (1 for historical, 2+ for incremental)
            target_table: Full target table name
            
        Returns:
            DataFrame with raw XML structure
            
        Raises:
            RuntimeError: If XML cannot be read
        """
        logger.info(f"Loading bronze_customer_mgmt from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/CustomerMgmt.xml"
        
        # Read XML with spark-xml, keeping full nested structure
        # Try different rowTag options based on TPC-DI XML format
        df = None
        for row_tag, root_tag in [("TPCDI:Action", "TPCDI:Actions"), ("Action", None)]:
            try:
                opts = {"format": "xml", "rowTag": row_tag}
                if root_tag:
                    opts["rootTag"] = root_tag
                df = self.platform.read_raw_file(file_path, **opts)
                if df.count() > 0:
                    logger.info(f"Successfully read XML with rowTag={row_tag}")
                    break
                df = None
            except Exception as e:
                logger.warning(f"Failed to read XML with rowTag={row_tag}: {e}")
                df = None
        
        if df is None or df.count() == 0:
            raise RuntimeError(
                f"Could not read CustomerMgmt.xml from Batch{batch_id}.\n"
                f"Ensure spark-xml library is installed (com.databricks:spark-xml_2.12:0.15.0)"
            )
        
        return self._write_bronze_table(df, target_table, batch_id, "CustomerMgmt.xml")
