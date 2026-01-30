"""
Parallel parsing of CustomerMgmt.xml using a Python UDTF on Databricks.

Splits the XML file into chunks (by Action elements), then uses a UDTF to parse
each chunk in parallel and yield one row per Action. Use when Spark 3.5+ and
Databricks to parallelize parsing; falls back to spark-xml read if UDTF unavailable.
"""

import logging
import re
from typing import Iterator, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

# Delimiter used when joining Action XML strings into a chunk (must not appear inside XML)
ACTION_BOUNDARY = "\n<!--ACTION_BOUNDARY-->\n"


def _split_actions(content: str) -> list:
    """Extract individual Action elements from XML content. Handles TPCDI:Action or Action."""
    # Match <Action ...>...</Action> or <TPCDI:Action ...>...</TPCDI:Action>
    pattern = re.compile(
        r"(<(?:(?:TPCDI:)?Action)[^>]*>.*?</(?:(?:TPCDI:)?Action)>)",
        re.DOTALL,
    )
    return pattern.findall(content)


def _chunk_list(lst: list, k: int) -> list:
    """Partition list into k roughly equal chunks."""
    if k <= 0 or not lst:
        return [lst] if lst else []
    n = len(lst)
    k = min(k, n)
    size, remainder = divmod(n, k)
    chunks = []
    start = 0
    for i in range(k):
        end = start + size + (1 if i < remainder else 0)
        chunks.append(lst[start:end])
        start = end
    return chunks


def read_customer_mgmt_with_udtf(
    spark,
    full_path: str,
    num_chunks: int = 64,
    row_tag: str = "TPCDI:Action",
    root_tag: Optional[str] = "TPCDI:Actions",
) -> Optional["DataFrame"]:
    """
    Read CustomerMgmt.xml using a UDTF to parse chunks in parallel.
    Requires Spark 3.5+ (e.g. Databricks DBR 14.3+). Returns a DataFrame
    with the same schema as spark-xml read (nested struct per Action).
    """
    from pyspark.sql.functions import col, udtf

    # 1) Get schema from a tiny spark-xml read (same as current loader)
    try:
        schema_df = (
            spark.read.format("xml")
            .option("rowTag", row_tag)
            .option("rootTag", root_tag or "root")
            .load(full_path)
            .limit(1)
        )
        action_schema = schema_df.schema
    except Exception as e:
        logger.warning(f"Could not get schema from spark-xml for UDTF path: {e}")
        return None

    # 2) Read file as binary, split into chunks on the executor (avoid driver OOM)
    binary_df = spark.read.format("binaryFile").load(full_path)
    num_partitions = max(1, min(num_chunks, 256))

    def _partition_to_chunks(rows):
        for row in rows:
            content = row.content
            if not content:
                continue
            content_str = content.decode("utf-8", errors="replace")
            actions = _split_actions(content_str)
            if not actions:
                continue
            chunks = _chunk_list(actions, num_partitions)
            for i, chunk_actions in enumerate(chunks):
                yield (i, ACTION_BOUNDARY.join(chunk_actions))

    from pyspark.sql.types import IntegerType, StringType
    from pyspark.sql import Row
    chunk_rdd = binary_df.rdd.mapPartitions(_partition_to_chunks)
    chunks_df = spark.createDataFrame(
        chunk_rdd.map(lambda x: Row(chunk_id=x[0], chunk_content=x[1])),
        "chunk_id: int, chunk_content: string",
    ).repartition(num_partitions)
    chunks_df.createOrReplaceTempView("_customer_mgmt_chunks")

    # 4) UDTF that takes chunk_content and yields (action_ordinal, action_xml)
    @udtf(returnType="action_ordinal: int, action_xml: string")
    class ParseCustomerMgmtChunk:
        def eval(self, chunk_id: int, chunk_content: str) -> Iterator[tuple]:
            if not chunk_content:
                return
            parts = chunk_content.split(ACTION_BOUNDARY)
            for i, action_xml in enumerate(parts):
                action_xml = action_xml.strip()
                if action_xml:
                    yield (i, action_xml)

    spark.udtf.register("parse_customer_mgmt_chunk", ParseCustomerMgmtChunk)

    # 5) Lateral join: each chunk row produces multiple (action_ordinal, action_xml) rows.
    # Use LATERAL VIEW ... AS (Databricks SQL requires VIEW; plain LATERAL causes PARSE_SYNTAX_ERROR).
    lateral_sql = """
        SELECT c.chunk_id, p.action_ordinal, p.action_xml
        FROM _customer_mgmt_chunks c
        LATERAL VIEW parse_customer_mgmt_chunk(c.chunk_id, c.chunk_content) p AS action_ordinal, action_xml
    """
    parsed_rows = spark.sql(lateral_sql)

    # 6) Parse action_xml into struct using from_xml (Spark 4.0+ / DBR 14.3+ with spark-xml)
    try:
        from pyspark.sql import functions as F
        from_xml = getattr(F, "from_xml", None)
        if from_xml is not None:
            parsed_rows = parsed_rows.withColumn(
                "_parsed", from_xml(col("action_xml"), action_schema)
            )
            # Flatten to match spark-xml read: select struct columns as top-level
            struct_fields = parsed_rows.select("_parsed.*").schema.fields
            parsed_rows = parsed_rows.select(
                *[col("_parsed." + f.name).alias(f.name) for f in struct_fields],
            )
            return parsed_rows
    except Exception as e:
        logger.warning(f"from_xml not available for UDTF path: {e}")

    # Fallback: from_xml not available (e.g. Spark 3.5 without backport)
    return None
