# Databricks notebook source
# Transform bronze_customer_mgmt -> silver_customers (PySpark: Pattern 1/2)
dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("batch_id", "1", "Batch ID")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

from pyspark.sql.functions import col, lit, when, to_timestamp, explode, coalesce, expr
from pyspark.sql.types import TimestampType

bronze_df = spark.table(f"{catalog}.{schema_name}.bronze_customer_mgmt").filter(col("_batch_id") == batch_id)
customer_df = None
try:
    customer_df = bronze_df.select(
        col("_ActionType").alias("action_type"), col("_ActionTS").alias("action_ts"),
        col("Customer._C_ID").alias("c_id"), col("Customer._C_TAX_ID").alias("c_tax_id"),
        col("Customer._C_GNDR").alias("c_gndr"), col("Customer._C_TIER").alias("c_tier"),
        col("Customer._C_DOB").alias("c_dob"),
        col("Customer.Name.C_L_NAME").alias("c_l_name"), col("Customer.Name.C_F_NAME").alias("c_f_name"),
        col("Customer.Name.C_M_NAME").alias("c_m_name"),
        col("Customer.Address.C_ADLINE1").alias("c_adline1"), col("Customer.Address.C_ADLINE2").alias("c_adline2"),
        col("Customer.Address.C_ZIPCODE").alias("c_zipcode"), col("Customer.Address.C_CITY").alias("c_city"),
        col("Customer.Address.C_STATE_PROV").alias("c_state_prov"), col("Customer.Address.C_CTRY").alias("c_ctry"),
        col("Customer.ContactInfo.C_PRIM_EMAIL").alias("c_prim_email"), col("Customer.ContactInfo.C_ALT_EMAIL").alias("c_alt_email"),
        col("Customer.TaxInfo.C_LCL_TX_ID").alias("c_lcl_tx_id"), col("Customer.TaxInfo.C_NAT_TX_ID").alias("c_nat_tx_id"),
        col("_batch_id").alias("batch_id"), col("_load_timestamp").alias("load_timestamp"),
    ).filter(col("c_id").isNotNull())
    if customer_df.count() == 0:
        customer_df = None
except Exception:
    customer_df = None
if customer_df is None:
    for cname in bronze_df.columns:
        if "array" in str(bronze_df.schema[cname].dataType).lower():
            try:
                exploded = bronze_df.select(explode(col(cname)).alias("Action"), col("_batch_id"), col("_load_timestamp"))
                customer_df = exploded.select(
                    col("Action._ActionType").alias("action_type"), col("Action._ActionTS").alias("action_ts"),
                    col("Action.Customer._C_ID").alias("c_id"), col("Action.Customer._C_TAX_ID").alias("c_tax_id"),
                    col("Action.Customer._C_GNDR").alias("c_gndr"), col("Action.Customer._C_TIER").alias("c_tier"),
                    col("Action.Customer._C_DOB").alias("c_dob"),
                    col("Action.Customer.Name.C_L_NAME").alias("c_l_name"), col("Action.Customer.Name.C_F_NAME").alias("c_f_name"),
                    col("Action.Customer.Name.C_M_NAME").alias("c_m_name"),
                    col("Action.Customer.Address.C_ADLINE1").alias("c_adline1"), col("Action.Customer.Address.C_ADLINE2").alias("c_adline2"),
                    col("Action.Customer.Address.C_ZIPCODE").alias("c_zipcode"), col("Action.Customer.Address.C_CITY").alias("c_city"),
                    col("Action.Customer.Address.C_STATE_PROV").alias("c_state_prov"), col("Action.Customer.Address.C_CTRY").alias("c_ctry"),
                    col("Action.Customer.ContactInfo.C_PRIM_EMAIL").alias("c_prim_email"), col("Action.Customer.ContactInfo.C_ALT_EMAIL").alias("c_alt_email"),
                    col("Action.Customer.TaxInfo.C_LCL_TX_ID").alias("c_lcl_tx_id"), col("Action.Customer.TaxInfo.C_NAT_TX_ID").alias("c_nat_tx_id"),
                    col("_batch_id").alias("batch_id"), col("_load_timestamp").alias("load_timestamp"),
                ).filter(col("Action.Customer._C_ID").isNotNull())
                if customer_df.count() > 0:
                    break
            except Exception:
                continue
if customer_df is None or customer_df.count() == 0:
    raise RuntimeError("Failed to extract customers from bronze_customer_mgmt")
silver_customers = customer_df.select(
    expr("try_cast(c_id AS BIGINT)").alias("sk_customer_id"), expr("try_cast(c_id AS BIGINT)").alias("customer_id"),
    col("c_tax_id").alias("tax_id"), col("action_type").alias("status"),
    col("c_l_name").alias("last_name"), col("c_f_name").alias("first_name"), col("c_m_name").alias("middle_name"),
    when(col("c_gndr").isin("M", "m"), lit("Male")).when(col("c_gndr").isin("F", "f"), lit("Female")).otherwise(lit("Unknown")).alias("gender"),
    expr("try_cast(c_tier AS INT)").alias("tier"), expr("try_cast(c_dob AS DATE)").alias("dob"),
    col("c_adline1").alias("address_line1"), col("c_adline2").alias("address_line2"), col("c_zipcode").alias("postal_code"),
    col("c_city").alias("city"), col("c_state_prov").alias("state_prov"), col("c_ctry").alias("country"),
    col("c_prim_email").alias("email1"), col("c_alt_email").alias("email2"),
    col("c_lcl_tx_id").alias("local_tax_id"), col("c_nat_tx_id").alias("national_tax_id"),
    when(col("action_type") == "INACT", lit(False)).otherwise(lit(True)).alias("is_current"),
    to_timestamp(col("action_ts")).alias("effective_date"), lit(None).cast(TimestampType()).alias("end_date"),
    col("batch_id"), col("load_timestamp"), coalesce(col("action_type"), lit("I")).alias("record_type"),
)
silver_customers.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.silver_customers")
