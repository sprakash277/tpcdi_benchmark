# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Bronze to Silver (Incremental)
# MAGIC
# MAGIC Transforms incremental Bronze data to Silver layer

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw Data Path")
dbutils.widgets.text("sf", "10", "Scale Factor")
dbutils.widgets.text("batch_id", "1", "Batch ID")
dbutils.widgets.text("xml_format", "com.databricks.spark.xml", "XML Format")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = int(dbutils.widgets.get("batch_id"))

# Construct full path with sf appended
full_raw_data_path = f"{raw_data_path}/sf={sf}"

# Set SQL variables
spark.sql(f"SET var.catalog = '{catalog}'")
spark.sql(f"SET var.schema = '{schema_name}'")
spark.sql(f"SET var.raw_data_path = '{full_raw_data_path}'")
spark.sql(f"SET var.batch_id = {batch_id}")
spark.sql(f"SET var.sf = {sf}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Tables
# MAGIC
# MAGIC Create all silver tables before transforming data.

# COMMAND ----------

# Get the current notebook path to determine the base path for table creation notebooks
import os
current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
base_path = os.path.dirname(current_notebook_path)
tables_path = f"{base_path}/tables"

# List of all silver tables to create (in order)
silver_tables = [
    "silver_date",
    "silver_time",
    "silver_status_type",
    "silver_trade_type",
    "silver_industry",
    "silver_tax_rate",
    "silver_companies",
    "silver_securities",
    "silver_financials",
    "silver_customers",
    "silver_accounts",
    "silver_trades",
    "silver_daily_market",
    "silver_prospect",
    "silver_cash_transaction",
    "silver_watch_history",
    "silver_holding_history"
]

# Create all silver tables
for table_name in silver_tables:
    create_notebook = f"{tables_path}/create_{table_name}"
    print(f"Creating table: {table_name} via {create_notebook}")
    try:
        dbutils.notebook.run(create_notebook, timeout_seconds=300, arguments={
            "catalog": catalog,
            "schema_name": schema_name
        })
    except Exception as e:
        # If table already exists, that's okay (CREATE TABLE IF NOT EXISTS handles this)
        if "already exists" not in str(e).lower() and "table" not in str(e).lower():
            print(f"Warning: Error creating {table_name}: {e}")
            raise

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- TPC-DI v2: Silver Layer - Incremental Transformations (Batch 2+)
# MAGIC -- ============================================================================
# MAGIC -- Transforms Bronze raw data into Silver with SCD Type 2 MERGE logic
# MAGIC -- Batch 2+: Incremental load (MERGE for SCD Type 2 tables)
# MAGIC -- ============================================================================
# MAGIC -- Set variables
# MAGIC -- SET var.batch_id = 2;  -- Change for Batch 3, 4, etc.
# MAGIC -- ============================================================================
# MAGIC -- Brokerage Data: Parse Customer.txt and Account.txt (Batch 2+)
# MAGIC -- ============================================================================
# COMMAND ----------

# Set catalog and create/use schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

# silver_customers: Parse Customer.txt with SCD Type 2 MERGE (use temp view so INSERT can reference it)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW incoming_customers AS
SELECT 
    monotonically_increasing_id() AS sk_customer_id,
    CAST(split(raw_line, '\\\\|')[2] AS BIGINT) AS customer_id,
    split(raw_line, '\\\\|')[3] AS tax_id,
    split(raw_line, '\\\\|')[4] AS status,
    split(raw_line, '\\\\|')[5] AS last_name,
    split(raw_line, '\\\\|')[6] AS first_name,
    split(raw_line, '\\\\|')[7] AS middle_name,
    split(raw_line, '\\\\|')[8] AS gender,
    CAST(split(raw_line, '\\\\|')[9] AS INT) AS tier,
    CAST(split(raw_line, '\\\\|')[10] AS DATE) AS dob,
    split(raw_line, '\\\\|')[11] AS address_line1,
    split(raw_line, '\\\\|')[12] AS address_line2,
    split(raw_line, '\\\\|')[13] AS postal_code,
    split(raw_line, '\\\\|')[14] AS city,
    split(raw_line, '\\\\|')[15] AS state_prov,
    split(raw_line, '\\\\|')[16] AS country,
    split(raw_line, '\\\\|')[17] AS email1,
    split(raw_line, '\\\\|')[18] AS email2,
    split(raw_line, '\\\\|')[19] AS local_tax_id,
    split(raw_line, '\\\\|')[20] AS national_tax_id,
    split(raw_line, '\\\\|')[0] AS cdc_flag,
    CAST(split(raw_line, '\\\\|')[1] AS TIMESTAMP) AS cdc_dsn,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_customer
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\\\|')) >= 21
""")

spark.sql(f"""
MERGE INTO {catalog}.{schema_name}.silver_customers AS target
USING (
    SELECT customer_id, MIN(cdc_dsn) AS new_effective_date
    FROM incoming_customers
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY customer_id
) AS updates
ON target.customer_id = updates.customer_id AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date
""")

spark.sql(f"""
INSERT INTO {catalog}.{schema_name}.silver_customers
SELECT 
    sk_customer_id,
    customer_id,
    tax_id,
    status,
    last_name,
    first_name,
    middle_name,
    gender,
    tier,
    dob,
    address_line1,
    address_line2,
    postal_code,
    city,
    state_prov,
    country,
    email1,
    email2,
    local_tax_id,
    national_tax_id,
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    NULL AS end_date,
    batch_id,
    load_timestamp,
    cdc_flag AS record_type
FROM incoming_customers
WHERE cdc_flag IN ('I', 'U')
""")

# COMMAND ----------

# silver_accounts: Parse Account.txt with SCD Type 2 MERGE (use temp view so INSERT can reference it)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW incoming_accounts AS
SELECT 
    CAST(split(raw_line, '\\\\|')[2] AS BIGINT) AS account_id,
    CAST(split(raw_line, '\\\\|')[3] AS BIGINT) AS broker_id,
    CAST(split(raw_line, '\\\\|')[4] AS BIGINT) AS customer_id,
    split(raw_line, '\\\\|')[5] AS account_name,
    CAST(split(raw_line, '\\\\|')[6] AS INT) AS tax_status,
    split(raw_line, '\\\\|')[7] AS status_id,
    split(raw_line, '\\\\|')[0] AS cdc_flag,
    CAST(split(raw_line, '\\\\|')[1] AS TIMESTAMP) AS cdc_dsn,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_account
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\\\|')) >= 8
""")

spark.sql(f"""
MERGE INTO {catalog}.{schema_name}.silver_accounts AS target
USING (
    SELECT account_id, MIN(cdc_dsn) AS new_effective_date
    FROM incoming_accounts
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY account_id
) AS updates
ON target.account_id = updates.account_id AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date
""")

spark.sql(f"""
INSERT INTO {catalog}.{schema_name}.silver_accounts
SELECT 
    account_id,
    broker_id,
    customer_id,
    account_name,
    tax_status,
    status_id,
    NULL AS action_type,
    cdc_dsn AS action_timestamp,
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    NULL AS end_date,
    batch_id,
    load_timestamp,
    cdc_flag AS record_type
FROM incoming_accounts
WHERE cdc_flag IN ('I', 'U')
""")

# COMMAND ----------

# silver_trades: temp view + MERGE + INSERT (one cell)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW incoming_trades AS
SELECT 
    CAST(split(raw_line, '\\\\|')[2] AS BIGINT) AS trade_id,
    CAST(split(raw_line, '\\\\|')[3] AS TIMESTAMP) AS trade_dts,
    split(raw_line, '\\\\|')[4] AS status_id,
    split(raw_line, '\\\\|')[5] AS trade_type_id,
    CAST(split(raw_line, '\\\\|')[6] AS BOOLEAN) AS is_cash,
    split(raw_line, '\\\\|')[7] AS symbol,
    CAST(split(raw_line, '\\\\|')[8] AS INT) AS quantity,
    CAST(split(raw_line, '\\\\|')[9] AS DOUBLE) AS bid_price,
    CAST(split(raw_line, '\\\\|')[10] AS BIGINT) AS account_id,
    split(raw_line, '\\\\|')[11] AS exec_name,
    CAST(split(raw_line, '\\\\|')[12] AS DOUBLE) AS trade_price,
    CAST(split(raw_line, '\\\\|')[13] AS DOUBLE) AS charge,
    CAST(split(raw_line, '\\\\|')[14] AS DOUBLE) AS commission,
    CAST(split(raw_line, '\\\\|')[15] AS DOUBLE) AS tax,
    split(raw_line, '\\\\|')[0] AS cdc_flag,
    CAST(split(raw_line, '\\\\|')[1] AS TIMESTAMP) AS cdc_dsn,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_trade
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\\\|')) = 18
""")
spark.sql(f"""
MERGE INTO {catalog}.{schema_name}.silver_trades AS target
USING (
    SELECT trade_id, MIN(cdc_dsn) AS new_effective_date
    FROM incoming_trades
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY trade_id
) AS updates
ON target.trade_id = updates.trade_id AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date
""")
spark.sql(f"""
INSERT INTO {catalog}.{schema_name}.silver_trades
SELECT 
    trade_id,
    trade_dts,
    status_id,
    trade_type_id,
    is_cash,
    symbol,
    quantity,
    bid_price,
    account_id,
    exec_name,
    trade_price,
    charge,
    commission,
    tax,
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    NULL AS end_date,
    batch_id,
    load_timestamp,
    cdc_flag AS record_type
FROM incoming_trades
WHERE cdc_flag IN ('I', 'U')
""")

# COMMAND ----------

spark.sql(f"""
MERGE INTO {catalog}.{schema_name}.silver_daily_market AS target
USING (
    SELECT 
        CONCAT(CAST(split(raw_line, '\\\\|')[2] AS DATE), '|', split(raw_line, '\\\\|')[3]) AS dm_key,
        CAST(split(raw_line, '\\\\|')[2] AS DATE) AS dm_date,
        split(raw_line, '\\\\|')[3] AS dm_s_symb,
        CAST(split(raw_line, '\\\\|')[4] AS DOUBLE) AS dm_close,
        CAST(split(raw_line, '\\\\|')[5] AS DOUBLE) AS dm_high,
        CAST(split(raw_line, '\\\\|')[6] AS DOUBLE) AS dm_low,
        CAST(split(raw_line, '\\\\|')[7] AS BIGINT) AS dm_vol,
        {batch_id} AS batch_id,
        current_timestamp() AS load_timestamp
    FROM {catalog}.{schema_name}.bronze_daily_market
    WHERE _batch_id = {batch_id}
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '\\\\|')) = 8
) AS source
ON target.dm_key = source.dm_key
WHEN MATCHED THEN UPDATE SET
    target.dm_close = source.dm_close,
    target.dm_high = source.dm_high,
    target.dm_low = source.dm_low,
    target.dm_vol = source.dm_vol,
    target.batch_id = source.batch_id,
    target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# silver_cash_transaction: temp view + MERGE + INSERT (one cell)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW incoming_cash AS
SELECT 
    CONCAT(CAST(split(raw_line, '\\\\|')[2] AS BIGINT), '|', CAST(split(raw_line, '\\\\|')[3] AS TIMESTAMP)) AS ct_key,
    CAST(split(raw_line, '\\\\|')[2] AS BIGINT) AS ct_ca_id,
    CAST(split(raw_line, '\\\\|')[3] AS TIMESTAMP) AS ct_dts,
    CAST(split(raw_line, '\\\\|')[4] AS DOUBLE) AS ct_amt,
    split(raw_line, '\\\\|')[5] AS ct_name,
    split(raw_line, '\\\\|')[0] AS cdc_flag,
    CAST(split(raw_line, '\\\\|')[1] AS TIMESTAMP) AS cdc_dsn,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_cash_transaction
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\\\|')) = 6
""")
spark.sql(f"""
MERGE INTO {catalog}.{schema_name}.silver_cash_transaction AS target
USING (
    SELECT ct_key, MIN(cdc_dsn) AS new_effective_date
    FROM incoming_cash
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY ct_key
) AS updates
ON target.ct_key = updates.ct_key AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date
""")
spark.sql(f"""
INSERT INTO {catalog}.{schema_name}.silver_cash_transaction
SELECT 
    ct_key,
    ct_ca_id,
    ct_dts,
    ct_amt,
    ct_name,
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    NULL AS end_date,
    batch_id,
    load_timestamp,
    cdc_flag AS record_type
FROM incoming_cash
WHERE cdc_flag IN ('I', 'U')
""")

# COMMAND ----------

# silver_holding_history: temp view + MERGE + INSERT (one cell)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW incoming_holdings AS
SELECT 
    CAST(split(raw_line, '\\\\|')[2] AS BIGINT) AS hh_h_t_id,
    CAST(split(raw_line, '\\\\|')[3] AS BIGINT) AS hh_t_id,
    CAST(split(raw_line, '\\\\|')[4] AS INT) AS hh_before_qty,
    CAST(split(raw_line, '\\\\|')[5] AS INT) AS hh_after_qty,
    split(raw_line, '\\\\|')[0] AS cdc_flag,
    CAST(split(raw_line, '\\\\|')[1] AS TIMESTAMP) AS cdc_dsn,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_holding_history
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\\\|')) = 6
""")
spark.sql(f"""
MERGE INTO {catalog}.{schema_name}.silver_holding_history AS target
USING (
    SELECT hh_h_t_id, MIN(cdc_dsn) AS new_effective_date
    FROM incoming_holdings
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY hh_h_t_id
) AS updates
ON target.hh_h_t_id = updates.hh_h_t_id AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date
""")
spark.sql(f"""
INSERT INTO {catalog}.{schema_name}.silver_holding_history
SELECT 
    hh_h_t_id,
    hh_t_id,
    hh_before_qty,
    hh_after_qty,
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    NULL AS end_date,
    batch_id,
    load_timestamp,
    cdc_flag AS record_type
FROM incoming_holdings
WHERE cdc_flag IN ('I', 'U')
""")

# COMMAND ----------

# silver_watch_history: temp view + MERGE + INSERT (one cell)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW incoming_watches AS
SELECT 
    CONCAT(CAST(split(raw_line, '\\\\|')[2] AS BIGINT), '|', split(raw_line, '\\\\|')[3]) AS wh_key,
    CAST(split(raw_line, '\\\\|')[2] AS BIGINT) AS w_c_id,
    split(raw_line, '\\\\|')[3] AS w_s_symb,
    CAST(split(raw_line, '\\\\|')[4] AS TIMESTAMP) AS w_dts,
    split(raw_line, '\\\\|')[5] AS w_action,
    split(raw_line, '\\\\|')[0] AS cdc_flag,
    CAST(split(raw_line, '\\\\|')[1] AS TIMESTAMP) AS cdc_dsn,
    {batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_watch_history
WHERE _batch_id = {batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\\\|')) = 6
""")
spark.sql(f"""
MERGE INTO {catalog}.{schema_name}.silver_watch_history AS target
USING (
    SELECT wh_key, MIN(cdc_dsn) AS new_effective_date
    FROM incoming_watches
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY wh_key
) AS updates
ON target.wh_key = updates.wh_key AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date
""")
spark.sql(f"""
INSERT INTO {catalog}.{schema_name}.silver_watch_history
SELECT 
    wh_key,
    w_c_id,
    w_s_symb,
    w_dts,
    w_action,
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    NULL AS end_date,
    batch_id,
    load_timestamp,
    cdc_flag AS record_type
FROM incoming_watches
WHERE cdc_flag IN ('I', 'U')
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Other Sources: Prospect (Batch 2+)
# MAGIC -- ============================================================================
# MAGIC -- silver_prospect: Parse Prospect.csv (append for incremental)
# MAGIC INSERT INTO silver_prospect
# MAGIC SELECT 
# MAGIC     split(raw_line, ',')[0] AS agency_id,
# MAGIC     split(raw_line, ',')[1] AS last_name,
# MAGIC     split(raw_line, ',')[2] AS first_name,
# MAGIC     split(raw_line, ',')[3] AS middle_initial,
# MAGIC     split(raw_line, ',')[4] AS gender,
# MAGIC     split(raw_line, ',')[5] AS address_line1,
# MAGIC     split(raw_line, ',')[6] AS address_line2,
# MAGIC     split(raw_line, ',')[7] AS postal_code,
# MAGIC     split(raw_line, ',')[8] AS city,
# MAGIC     split(raw_line, ',')[9] AS state,
# MAGIC     split(raw_line, ',')[10] AS country,
# MAGIC     split(raw_line, ',')[11] AS phone,
# MAGIC     CAST(split(raw_line, ',')[12] AS INT) AS income,
# MAGIC     CAST(split(raw_line, ',')[13] AS INT) AS number_cars,
# MAGIC     CAST(split(raw_line, ',')[14] AS INT) AS number_children,
# MAGIC     split(raw_line, ',')[15] AS marital_status,
# MAGIC     CAST(split(raw_line, ',')[16] AS INT) AS age,
# MAGIC     CAST(split(raw_line, ',')[17] AS INT) AS credit_rating,
# MAGIC     split(raw_line, ',')[18] AS own_or_rent_flag,
# MAGIC     split(raw_line, ',')[19] AS employer,
# MAGIC     CAST(split(raw_line, ',')[20] AS INT) AS number_credit_cards,
# MAGIC     CAST(split(raw_line, ',')[21] AS INT) AS net_worth,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC FROM bronze_prospect
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != '';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Load completed' AS status;