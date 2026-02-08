"""
Bronze layer loader for CustomerMgmt.xml.

Ingests raw XML structure from CustomerMgmt.xml with no field extraction.
- On Databricks (Spark 3.5+): can use a Python UDTF to parallelize parsing (see use_udtf).
- Otherwise: uses spark-xml (native Spark DataFrameReader). No pandas UDF.
- Schema: if customer_mgmt_schema.json exists next to this module, it is used (no inference).
  On first run without that file, schema is inferred, printed, and saved for next time.
"""

import json
import logging
import os
from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from benchmark.etl.bronze.base import BronzeLoaderBase

logger = logging.getLogger(__name__)

_SCHEMA_FILENAME = "customer_mgmt_schema.json"


def _schema_path() -> Path:
    """Path to schema JSON. Use writable dir: module dir when not running from zip, else cwd or env."""
    module_dir = Path(__file__).resolve().parent
    # When run from a zip (e.g. Dataproc --py-files=benchmark.zip), module_dir is inside the zip and not writable
    if module_dir.is_dir() and os.access(module_dir, os.W_OK):
        return module_dir / _SCHEMA_FILENAME
    fallback = os.environ.get("TPCDI_CUSTOMERMGMT_SCHEMA_DIR")
    if fallback:
        return Path(fallback).resolve() / _SCHEMA_FILENAME
    return Path.cwd() / _SCHEMA_FILENAME


def _load_customer_mgmt_schema() -> Optional[StructType]:
    """Load CustomerMgmt schema from JSON file if it exists (module dir or writable fallback)."""
    module_dir = Path(__file__).resolve().parent
    for path in [
        module_dir / _SCHEMA_FILENAME,  # repo/module dir when not from zip
        _schema_path(),  # writable fallback (cwd or env) when running from zip
    ]:
        if path.is_file():
            break
    else:
        return None
    try:
        with open(path, "r") as f:
            data = json.load(f)
        # Pass dict; fromJson() in PySpark accepts dict (Databricks) or str (some versions)
        if isinstance(data, dict):
            return StructType.fromJson(data)
        return StructType.fromJson(json.dumps(data))
    except Exception as e:
        logger.warning(f"Could not load schema from {path}: {e}")
        return None


def _save_customer_mgmt_schema(schema_json: str) -> None:
    """Save schema JSON to file for next run (avoids inference)."""
    path = _schema_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(schema_json)
        logger.info(f"Saved CustomerMgmt schema to {path} for next run")
    except Exception as e:
        logger.warning(f"Could not save schema to {path}: {e}")


def _print_customer_mgmt_schema(df: DataFrame, always_print: bool = False) -> None:
    """Print CustomerMgmt.xml schema (JSON + DDL). Set always_print=True when we just inferred; else only if env var set."""
    if not always_print and os.environ.get("TPCDI_PRINT_CUSTOMERMGMT_SCHEMA") != "1":
        return
    try:
        schema_json = df.schema.json()
        schema_ddl = df.schema.simpleString()
        print("\n" + "=" * 80)
        print("CustomerMgmt.xml schema (saved to customer_mgmt_schema.json for next run)")
        print("=" * 80)
        print("\n# JSON (StructType.fromJson):")
        print(schema_json)
        print("\n# DDL (simpleString):")
        print(schema_ddl)
        print("=" * 80 + "\n")
    except Exception as e:
        logger.warning(f"Could not print schema: {e}")


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
        use_udtf: bool = False,
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
                # Derive catalog and schema from target_table so UDTF is registered there (avoids ROUTINE_NOT_FOUND).
                parts = target_table.split(".")
                udtf_catalog = parts[0] if len(parts) >= 3 else None
                udtf_schema = parts[1] if len(parts) >= 3 else (parts[0] if len(parts) == 2 else None)
                df = customer_mgmt_udtf.read_customer_mgmt_with_udtf(
                    self.spark,
                    full_path,
                    num_chunks=udtf_num_chunks,
                    row_tag="TPCDI:Action",
                    root_tag="TPCDI:Actions",
                    catalog=udtf_catalog,
                    schema=udtf_schema,
                )
                if df is not None:
                    logger.info("Successfully read CustomerMgmt.xml via UDTF (parallel parsing)")
                    _print_customer_mgmt_schema(df)
                    return self._write_bronze_table(df, target_table, batch_id, "CustomerMgmt.xml")
            except Exception as e:
                logger.warning(f"UDTF path failed, falling back to spark-xml: {e}")
        
        # Read XML with spark-xml. Use saved schema if present (skips inference); else infer, print, and save.
        schema = _load_customer_mgmt_schema()
        df = None
        success = False
        used_schema = False
        for row_tag, root_tag in [("TPCDI:Action", "TPCDI:Actions"), ("Action", None)]:
            try:
                opts = {"format": "xml", "rowTag": row_tag}
                if root_tag:
                    opts["rootTag"] = root_tag
                if schema is not None:
                    opts["schema"] = schema
                    used_schema = True
                df = self.platform.read_raw_file(file_path, **opts)
                if df.count() > 0:
                    logger.info(
                        f"Successfully read XML with rowTag={row_tag}"
                        + (" (using saved schema)" if used_schema else " (inferred schema)")
                    )
                    success = True
                    break
                df = None
            except Exception as e:
                if schema is not None:
                    logger.warning(f"Read with saved schema failed, will infer: {e}")
                    schema = None
                    used_schema = False
                    continue
                logger.warning(f"Failed to read XML with rowTag={row_tag}: {e}")
                df = None

        if not success or df is None:
            raise RuntimeError(
                f"Could not read CustomerMgmt.xml from Batch{batch_id}.\n"
                f"Ensure spark-xml library is installed (com.databricks:spark-xml_2.12:0.15.0)"
            )
        if used_schema:
            _print_customer_mgmt_schema(df, always_print=False)
        else:
            _print_customer_mgmt_schema(df, always_print=True)
            _save_customer_mgmt_schema(df.schema.json())
        return self._write_bronze_table(df, target_table, batch_id, "CustomerMgmt.xml")
