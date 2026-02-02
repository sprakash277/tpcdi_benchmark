"""
DimMessages (gold.dim_messages) - TPC-DI audit table for DQ failures.

Every Silver DQ rule that triggers inserts a row here:
message_timestamp, batch_id, component_name, message_text, severity, source_table.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

logger = logging.getLogger(__name__)

DIM_MESSAGES_SCHEMA = StructType([
    StructField("message_timestamp", TimestampType(), False),
    StructField("batch_id", IntegerType(), True),
    StructField("component_name", StringType(), False),
    StructField("message_text", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("source_table", StringType(), False),
])


def ensure_dim_messages_exists(spark, table_name: str, platform) -> None:
    """
    Create gold.dim_messages if it does not exist.

    Args:
        spark: SparkSession
        table_name: Full table name (e.g. catalog.schema.gold_dim_messages)
        platform: Platform adapter for write_table
    """
    try:
        if spark.catalog.tableExists(table_name):
            logger.debug(f"DimMessages table already exists: {table_name}")
            return
    except Exception as e:
        logger.warning(f"Could not check table existence for {table_name}: {e}")
    # Create empty table with schema
    empty = spark.createDataFrame([], DIM_MESSAGES_SCHEMA)
    platform.write_table(empty, table_name, mode="overwrite")
    logger.info(f"Created DimMessages table: {table_name}")


def log_message(spark, platform, table_name: str, batch_id: Optional[int],
                component_name: str, message_text: str, severity: str = "Alert",
                source_table: str = "") -> None:
    """
    Append a single message to gold.dim_messages.

    Args:
        spark: SparkSession
        platform: Platform adapter for write_table
        table_name: gold_dim_messages full name
        batch_id: Batch number (optional)
        component_name: e.g. 'Silver_Customer_Validation'
        message_text: e.g. 'Invalid TaxID format'
        severity: 'Alert' or 'Reject'
        source_table: Silver table that was validated (e.g. silver_customers)
    """
    ensure_dim_messages_exists(spark, table_name, platform)
    row = spark.range(1).select(
        current_timestamp().alias("message_timestamp"),
        lit(batch_id).alias("batch_id"),
        lit(component_name).alias("component_name"),
        lit(message_text).alias("message_text"),
        lit(severity).alias("severity"),
        lit(source_table or "").alias("source_table"),
    )
    platform.write_table(row, table_name, mode="append")
    logger.info(f"DimMessages: [{severity}] {component_name} - {message_text}")


def log_messages(spark, platform, table_name: str, messages_df: DataFrame) -> None:
    """
    Append a DataFrame of messages to gold.dim_messages.
    DataFrame must have columns: message_timestamp, batch_id, component_name, message_text, severity, source_table.

    Args:
        spark: SparkSession
        platform: Platform adapter for write_table
        table_name: gold_dim_messages full name
        messages_df: DataFrame with DIM_MESSAGES_SCHEMA columns
    """
    if messages_df.isEmpty():
        return
    ensure_dim_messages_exists(spark, table_name, platform)
    platform.write_table(messages_df, table_name, mode="append")
    logger.info(f"DimMessages: appended {messages_df.count()} message(s)")
