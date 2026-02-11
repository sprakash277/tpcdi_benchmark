# Databricks notebook source
# Load bronze_finwire from Batch1 FINWIRE*.txt (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = int(dbutils.widgets.get("batch_id"))
full_raw_data_path = f"{raw_data_path}/sf={sf}"
batch1_path = f"{full_raw_data_path}/Batch1"

from pyspark.sql.functions import lit, current_timestamp, col, length

batch1_files = dbutils.fs.ls(batch1_path)
finwire_files = [
    f.path for f in batch1_files
    if "FINWIRE" in f.name.upper()
    and not f.name.lower().endswith(".csv")
    and (f.name.lower().endswith(".txt") or "." not in f.name)
]
if not finwire_files:
    raise FileNotFoundError(
        f"No FINWIRE files (excluding *.csv) found under {batch1_path}. "
        f"Listed: {[f.name for f in batch1_files][:30]}"
    )
df_finwire = spark.read.format("text").load(finwire_files)
df_finwire_bronze = df_finwire.withColumnRenamed("value", "raw_line") \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit("FINWIRE*")) \
    .filter(col("raw_line").isNotNull()).filter(col("raw_line") != "").filter(length(col("raw_line")) >= 18)

df_finwire_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.bronze_finwire")
