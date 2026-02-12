# Databricks notebook source
# Load bronze_customer_mgmt from Batch1/CustomerMgmt.xml (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = int(dbutils.widgets.get("batch_id"))
xml_format = (dbutils.widgets.get("xml_format") or "com.databricks.spark.xml").strip() or "xml"
full_raw_data_path = f"{raw_data_path}/sf={sf}"
xml_path = f"{full_raw_data_path}/Batch1/CustomerMgmt.xml"

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DateType, TimestampType
)
from pyspark.sql.functions import lit, current_timestamp

def get_customer_mgmt_schema():
    return StructType([
        StructField("Customer", StructType([
            StructField("Account", StructType([
                StructField("CA_B_ID", LongType(), True),
                StructField("CA_NAME", StringType(), True),
                StructField("_CA_ID", LongType(), True),
                StructField("_CA_TAX_ST", LongType(), True),
            ]), True),
            StructField("Address", StructType([
                StructField("C_ADLINE1", StringType(), True),
                StructField("C_ADLINE2", StringType(), True),
                StructField("C_CITY", StringType(), True),
                StructField("C_CTRY", StringType(), True),
                StructField("C_STATE_PROV", StringType(), True),
                StructField("C_ZIPCODE", StringType(), True),
            ]), True),
            StructField("ContactInfo", StructType([
                StructField("C_ALT_EMAIL", StringType(), True),
                StructField("C_PHONE_1", StructType([
                    StructField("C_AREA_CODE", LongType(), True),
                    StructField("C_CTRY_CODE", LongType(), True),
                    StructField("C_EXT", LongType(), True),
                    StructField("C_LOCAL", StringType(), True),
                ]), True),
                StructField("C_PHONE_2", StructType([
                    StructField("C_AREA_CODE", LongType(), True),
                    StructField("C_CTRY_CODE", LongType(), True),
                    StructField("C_EXT", LongType(), True),
                    StructField("C_LOCAL", StringType(), True),
                ]), True),
                StructField("C_PHONE_3", StructType([
                    StructField("C_AREA_CODE", LongType(), True),
                    StructField("C_CTRY_CODE", LongType(), True),
                    StructField("C_EXT", LongType(), True),
                    StructField("C_LOCAL", StringType(), True),
                ]), True),
                StructField("C_PRIM_EMAIL", StringType(), True),
            ]), True),
            StructField("Name", StructType([
                StructField("C_F_NAME", StringType(), True),
                StructField("C_L_NAME", StringType(), True),
                StructField("C_M_NAME", StringType(), True),
            ]), True),
            StructField("TaxInfo", StructType([
                StructField("C_LCL_TX_ID", StringType(), True),
                StructField("C_NAT_TX_ID", StringType(), True),
            ]), True),
            StructField("_C_DOB", DateType(), True),
            StructField("_C_GNDR", StringType(), True),
            StructField("_C_ID", LongType(), True),
            StructField("_C_TAX_ID", StringType(), True),
            StructField("_C_TIER", LongType(), True),
        ]), True),
        StructField("_ActionTS", TimestampType(), True),
        StructField("_ActionType", StringType(), True),
    ])

schema = get_customer_mgmt_schema()
fmt = xml_format
df = None
success = False

for row_tag, root_tag in [("TPCDI:Action", "TPCDI:Actions"), ("Action", None)]:
    if success:
        break
    try:
        reader = spark.read.format(fmt).option("rowTag", row_tag)
        if root_tag:
            reader = reader.option("rootTag", root_tag)
        if schema is not None:
            reader = reader.schema(schema)
        df = reader.load(xml_path)
        if df.count() > 0:
            success = True
            break
        df = None
    except Exception as e:
        err_msg = str(e)
        if fmt == "com.databricks.spark.xml" and (
            "ServiceConfigurationError" in err_msg or "Unable to get public no-arg constructor" in err_msg
        ):
            fmt = "xml"
            try:
                reader = spark.read.format(fmt).option("rowTag", row_tag)
                if root_tag:
                    reader = reader.option("rootTag", root_tag)
                if schema is not None:
                    reader = reader.schema(schema)
                df = reader.load(xml_path)
                if df.count() > 0:
                    success = True
                    break
            except Exception:
                pass
        if schema is not None:
            schema = None
            continue
        df = None

if not success or df is None:
    raise RuntimeError(
        f"Could not read CustomerMgmt.xml from {xml_path}. "
        "Ensure spark-xml is available (e.g. com.databricks:spark-xml_2.12:0.15.0)."
    )

df_bronze = df.withColumn("_batch_id", lit(batch_id)) \
    .withColumn("_load_timestamp", current_timestamp()) \
    .withColumn("_source_file", lit("CustomerMgmt.xml"))

spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema_name}.bronze_customer_mgmt")
df_bronze.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.bronze_customer_mgmt")
