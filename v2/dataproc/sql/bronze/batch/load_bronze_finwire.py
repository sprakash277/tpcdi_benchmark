# Dataproc: load bronze_finwire from Batch1 FINWIRE*.txt
# Expects globals: spark, database, full_raw_path, batch_id (set by run_tpcdi_batch.py)
from pyspark.sql.functions import lit, current_timestamp, col, length

batch1_path = f"{full_raw_path}/Batch1"
# GCS/Spark often supports path glob
try:
    df_finwire = spark.read.format("text").load(f"{batch1_path}/FINWIRE*")
except Exception:
    raise FileNotFoundError(f"No FINWIRE files found under {batch1_path}. Ensure path exists and FINWIRE*.txt is present.")
df_finwire_bronze = (
    df_finwire.withColumnRenamed("value", "raw_line")
    .withColumn("_batch_id", lit(batch_id))
    .withColumn("_load_timestamp", current_timestamp())
    .withColumn("_source_file", lit("FINWIRE*"))
    .filter(col("raw_line").isNotNull())
    .filter(col("raw_line") != "")
    .filter(length(col("raw_line")) >= 18)
)
df_finwire_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{database}.bronze_finwire")
