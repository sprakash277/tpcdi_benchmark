"""
Bronze layer loader for CustomerMgmt.xml.

Ingests raw XML structure from CustomerMgmt.xml with no field extraction.
- On Databricks (Spark 3.5+): can use a Python UDTF to parallelize parsing (see use_udtf).
- Otherwise: uses spark-xml (native Spark DataFrameReader). No pandas UDF.
This table often takes longer because XML parsing is heavier and the file is typically one large file.
"""

import logging
from pyspark.sql import DataFrame
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)


class BronzeCustomerMgmt(BronzeLoaderBase):
    """
    Bronze layer loader for CustomerMgmt.xml.
    
    The XML is read with spark-xml (or UDTF on Databricks) and stored as nested struct.
    No field extraction - just raw capture of the XML structure.
    """
    
    def load(
        self,
        batch_id: int,
        target_table: str,
        use_udtf: bool = True,
        udtf_num_chunks: int = 64,
    ) -> DataFrame:
        """
        Ingest CustomerMgmt.xml as raw XML structure.
        
        Args:
            batch_id: Batch number (1 for historical, 2+ for incremental)
            target_table: Full target table name
            use_udtf: If True and on Databricks (Spark 3.5+), use UDTF to parallelize parsing.
            udtf_num_chunks: Number of chunks for UDTF path (more chunks = more parallelism).
            
        Returns:
            DataFrame with raw XML structure
            
        Raises:
            RuntimeError: If XML cannot be read
        """
        logger.info(f"Loading bronze_customer_mgmt from Batch{batch_id}")
        
        file_path = f"Batch{batch_id}/CustomerMgmt.xml"
        full_path = self.platform._resolve_path(file_path)
        
        # Optional: try UDTF path on Databricks (Spark 3.5+) to parallelize parsing
        if use_udtf:
            try:
                from benchmark.etl.bronze import customer_mgmt_udtf
                df = customer_mgmt_udtf.read_customer_mgmt_with_udtf(
                    self.spark,
                    full_path,
                    num_chunks=udtf_num_chunks,
                    row_tag="TPCDI:Action",
                    root_tag="TPCDI:Actions",
                )
                if df is not None:
                    logger.info("Successfully read CustomerMgmt.xml via UDTF (parallel parsing)")
                    return self._write_bronze_table(df, target_table, batch_id, "CustomerMgmt.xml")
            except Exception as e:
                logger.warning(f"UDTF path failed, falling back to spark-xml: {e}")
        
        # Read XML with spark-xml, keeping full nested structure
        # Try different rowTag options based on TPC-DI XML format
        df = None
        success = False
        for row_tag, root_tag in [("TPCDI:Action", "TPCDI:Actions"), ("Action", None)]:
            try:
                opts = {"format": "xml", "rowTag": row_tag}
                if root_tag:
                    opts["rootTag"] = root_tag
                df = self.platform.read_raw_file(file_path, **opts)
                if df.count() > 0:
                    logger.info(f"Successfully read XML with rowTag={row_tag}")
                    success = True
                    break
                df = None
            except Exception as e:
                logger.warning(f"Failed to read XML with rowTag={row_tag}: {e}")
                df = None
        
        if not success or df is None:
            raise RuntimeError(
                f"Could not read CustomerMgmt.xml from Batch{batch_id}.\n"
                f"Ensure spark-xml library is installed (com.databricks:spark-xml_2.12:0.15.0)"
            )
        
        return self._write_bronze_table(df, target_table, batch_id, "CustomerMgmt.xml")
