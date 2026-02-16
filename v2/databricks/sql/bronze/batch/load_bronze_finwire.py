# Databricks notebook source
# Load bronze_finwire from Batch1 FINWIRE*.txt (widgets set by orchestrator)
# On serverless, gs:// paths require Unity Catalog external location or use a UC Volume path for raw_data_path.
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = int(dbutils.widgets.get("batch_id"))
full_raw_data_path = f"{raw_data_path}/sf={sf}"
batch1_path = f"{full_raw_data_path}/Batch1"

from pyspark.sql.functions import lit, current_timestamp, col, length, input_file_name, lower

# Try Spark glob FINWIRE* first (then filter out .csv); works with UC Volume/external location on serverless.
# Fall back to dbutils.fs.ls (which also excludes .csv).
df_finwire = None
try:
    df_finwire = spark.read.format("text").load(f"{batch1_path}/FINWIRE*")
    # Exclude rows from .csv files (glob may match FINWIRE*.csv)
    df_finwire = df_finwire.withColumn("_path", input_file_name()).filter(~lower(col("_path")).like("%.csv")).drop("_path")
except Exception:
    pass
if df_finwire is None:
    try:
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
    except FileNotFoundError:
        raise
    except Exception as e2:
        raise FileNotFoundError(
            f"Cannot read FINWIRE from {batch1_path}. "
            "On Databricks serverless, gs:// paths require a Unity Catalog external location for the bucket, "
            "or use a UC Volume for raw data and set raw_data_path to the volume path (e.g. /Volumes/catalog/schema/volume/tpcdi)."
        ) from e2
if df_finwire.isEmpty():
    raise FileNotFoundError(f"No FINWIRE data under {batch1_path} (glob matched but no rows).")
df_finwire_bronze = df_finwire.withColumnRenamed("value", "raw_line") \
    .withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit("FINWIRE*")) \
    .filter(col("raw_line").isNotNull()).filter(col("raw_line") != "").filter(length(col("raw_line")) >= 18)

df_finwire_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.bronze_finwire")
