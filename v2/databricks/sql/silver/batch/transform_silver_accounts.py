# Databricks notebook source
# Transform bronze_customer_mgmt -> silver_accounts (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
batch_id = int(dbutils.widgets.get("batch_id"))

from pyspark.sql.functions import col, lit, when, to_timestamp, coalesce, trim
from pyspark.sql.types import TimestampType

bronze_accounts_df = spark.table(f"{catalog}.{schema_name}.bronze_customer_mgmt").filter(col("_batch_id") == batch_id)
account_df = None

try:
    account_df = bronze_accounts_df.filter(
        col("_ActionType").isin("ADDACCT", "UPDACCT", "INACT")
        & col("Customer").isNotNull()
        & col("Customer.Account").isNotNull()
    ).select(
        col("_ActionType").alias("action_type"),
        col("_ActionTS").alias("action_ts"),
        col("Customer.Account._CA_ID").alias("ca_id"),
        col("Customer.Account.CA_B_ID").alias("ca_b_id"),
        col("Customer._C_ID").alias("c_id"),
        col("Customer.Account.CA_NAME").alias("ca_name"),
        col("Customer.Account._CA_TAX_ST").alias("ca_tax_st"),
        col("_batch_id").alias("batch_id"),
        col("_load_timestamp").alias("load_timestamp"),
    )
    if account_df.count() == 0:
        account_df = None
except Exception:
    account_df = None

if account_df is None:
    try:
        account_df = bronze_accounts_df.filter(
            col("_ActionType").isin("ADDACCT", "UPDACCT", "INACT")
            & col("Customer").isNotNull()
            & col("Customer.Account").isNotNull()
        ).select(
            col("_ActionType").alias("action_type"),
            col("_ActionTS").alias("action_ts"),
            col("Customer.Account._CA_ID").alias("ca_id"),
            col("Customer.Account.CA_B_ID").alias("ca_b_id"),
            col("Customer._C_ID").alias("c_id"),
            trim(coalesce(col("Customer.Account.CA_NAME"), lit(""))).alias("ca_name"),
            trim(coalesce(col("Customer.Account._CA_TAX_ST").cast("string"), lit("0"))).alias("ca_tax_st"),
            col("_batch_id").alias("batch_id"),
            col("_load_timestamp").alias("load_timestamp"),
        )
        if account_df.count() == 0:
            account_df = None
    except Exception:
        account_df = None

if account_df is None or account_df.count() == 0:
    raise RuntimeError("Failed to extract accounts from bronze_customer_mgmt (Strategy 1 and 2)")

silver_accounts = account_df.select(
    col("ca_id").cast("long").alias("account_id"),
    coalesce(col("ca_b_id"), lit(0)).cast("long").alias("broker_id"),
    coalesce(col("c_id"), lit(0)).cast("long").alias("customer_id"),
    coalesce(col("ca_name"), lit("")).alias("account_name"),
    coalesce(col("ca_tax_st"), lit(0)).cast("int").alias("tax_status"),
    when(col("action_type") == "INACT", lit("INACT")).otherwise(lit("ACTV")).alias("status_id"),
    lit(True).alias("is_current"),
    to_timestamp(col("action_ts")).alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date"),
    col("batch_id").cast("int"),
    col("load_timestamp"),
    coalesce(col("action_type"), lit("I")).alias("record_type"),
)
silver_accounts.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema_name}.silver_accounts")
