"""
v3 Bronze: PySpark load using v2 table/column names.
Same tables as v2: bronze_date, bronze_time, ..., bronze_prospect.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

# v2 BRONZE_BATCH_PATHS: table_short -> (path_suffix, source_file_label)
BRONZE_BATCH_PATHS = {
    "bronze_date": ("Batch1/Date.txt", "Date.txt"),
    "bronze_time": ("Batch1/Time.txt", "Time.txt"),
    "bronze_status_type": ("Batch1/StatusType.txt", "StatusType.txt"),
    "bronze_trade_type": ("Batch1/TradeType.txt", "TradeType.txt"),
    "bronze_industry": ("Batch1/Industry.txt", "Industry.txt"),
    "bronze_tax_rate": ("Batch1/TaxRate.txt", "TaxRate.txt"),
    "bronze_trade": ("Batch1/Trade.txt", "Trade.txt"),
    "bronze_daily_market": ("Batch1/DailyMarket.txt", "DailyMarket.txt"),
    "bronze_cash_transaction": ("Batch1/CashTransaction.txt", "CashTransaction.txt"),
    "bronze_holding_history": ("Batch1/HoldingHistory.txt", "HoldingHistory.txt"),
    "bronze_watch_history": ("Batch1/WatchHistory.txt", "WatchHistory.txt"),
    "bronze_hr": ("Batch1/HR.csv", "HR.csv"),
    "bronze_prospect": ("Batch1/Prospect.csv", "Prospect.csv"),
}


def _drop_table_and_path(spark, database: str, table: str, warehouse_dir: str) -> None:
    """Drop table and delete Delta path (v2-style for re-runs)."""
    spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
    table_path = f"{warehouse_dir}/{database}.db/{table}"
    try:
        jvm = spark._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = jvm.org.apache.hadoop.fs.Path(table_path)
        if fs.exists(path):
            fs.delete(path, True)
    except Exception as e:
        print(f"WARN: Could not delete path {table_path}: {e}")


def load_bronze_batch(
    spark: SparkSession,
    database: str,
    batch_id: int,
    full_raw_path: str,
    warehouse_dir: str,
    table_order: list,
) -> None:
    """
    Load all batch bronze tables (v2 names). Reads raw text from full_raw_path/{path_suffix}.
    table_order: list of table short names in load order (e.g. date, time, ... prospect).
    """
    for table_short in table_order:
        if table_short not in BRONZE_BATCH_PATHS:
            continue
        path_suffix, source_file = BRONZE_BATCH_PATHS[table_short]
        path = f"{full_raw_path}/{path_suffix}"
        df = spark.read.text(path).filter(col("value").isNotNull()).filter(col("value") != "")
        bronze = df.select(
            col("value").alias("raw_line"),
            lit(batch_id).alias("_batch_id"),
            current_timestamp().alias("_load_timestamp"),
            lit(source_file).alias("_source_file"),
        )
        _drop_table_and_path(spark, database, table_short, warehouse_dir)
        bronze.write.format("delta").mode("overwrite").saveAsTable(f"{database}.{table_short}")
