# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Bronze to Silver (Incremental)
# MAGIC
# MAGIC Transforms incremental Bronze data to Silver layer

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("raw_data_path", "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10", "Raw Data Path")
dbutils.widgets.text("batch_id", "1", "Batch ID")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
batch_id = int(dbutils.widgets.get("batch_id"))

# Set SQL variables
spark.sql(f"SET var.catalog = '{catalog}'")
spark.sql(f"SET var.schema = '{schema_name}'")
spark.sql(f"SET var.raw_data_path = '{raw_data_path}'")
spark.sql(f"SET var.batch_id = {batch_id}")

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
# MAGIC -- silver_customers: Parse Customer.txt with SCD Type 2 MERGE
# MAGIC -- Format: CDC_FLAG|CDC_DSN|C_ID|C_TAX_ID|C_ST_ID|C_L_NAME|...
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH incoming_customers AS (
# MAGIC     SELECT 
# MAGIC         monotonically_increasing_id() AS sk_customer_id,
# MAGIC         CAST(split(raw_line, '\\|')[2] AS BIGINT) AS customer_id,  -- Skip CDC_FLAG, CDC_DSN
# MAGIC         split(raw_line, '\\|')[3] AS tax_id,
# MAGIC         split(raw_line, '\\|')[4] AS status,
# MAGIC         split(raw_line, '\\|')[5] AS last_name,
# MAGIC         split(raw_line, '\\|')[6] AS first_name,
# MAGIC         split(raw_line, '\\|')[7] AS middle_name,
# MAGIC         split(raw_line, '\\|')[8] AS gender,
# MAGIC         CAST(split(raw_line, '\\|')[9] AS INT) AS tier,
# MAGIC         CAST(split(raw_line, '\\|')[10] AS DATE) AS dob,
# MAGIC         split(raw_line, '\\|')[11] AS address_line1,
# MAGIC         split(raw_line, '\\|')[12] AS address_line2,
# MAGIC         split(raw_line, '\\|')[13] AS postal_code,
# MAGIC         split(raw_line, '\\|')[14] AS city,
# MAGIC         split(raw_line, '\\|')[15] AS state_prov,
# MAGIC         split(raw_line, '\\|')[16] AS country,
# MAGIC         split(raw_line, '\\|')[17] AS email1,
# MAGIC         split(raw_line, '\\|')[18] AS email2,
# MAGIC         split(raw_line, '\\|')[19] AS local_tax_id,
# MAGIC         split(raw_line, '\\|')[20] AS national_tax_id,
# MAGIC         split(raw_line, '\\|')[0] AS cdc_flag,  -- I=Insert, U=Update, D=Delete
# MAGIC         CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS cdc_dsn,  -- Change timestamp
# MAGIC         ${var.batch_id} AS batch_id,
# MAGIC         current_timestamp() AS load_timestamp
# MAGIC     FROM bronze_customer
# MAGIC     WHERE _batch_id = ${var.batch_id}
# MAGIC       AND raw_line IS NOT NULL
# MAGIC       AND raw_line != ''
# MAGIC       AND size(split(raw_line, '\\|')) >= 21
# MAGIC ),
# MAGIC -- Close existing current records that have updates
# MAGIC updates_to_close AS (
# MAGIC     SELECT 
# MAGIC         customer_id,
# MAGIC         MIN(cdc_dsn) AS new_effective_date
# MAGIC     FROM incoming_customers
# MAGIC     WHERE cdc_flag IN ('U', 'D')  -- Updates and deletes
# MAGIC     GROUP BY customer_id
# MAGIC )
# MAGIC MERGE INTO silver_customers AS target
# MAGIC USING updates_to_close AS updates
# MAGIC ON target.customer_id = updates.customer_id 
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.end_date = updates.new_effective_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert new versions (I and U records)
# MAGIC INSERT INTO silver_customers
# MAGIC SELECT 
# MAGIC     sk_customer_id,
# MAGIC     customer_id,
# MAGIC     tax_id,
# MAGIC     status,
# MAGIC     last_name,
# MAGIC     first_name,
# MAGIC     middle_name,
# MAGIC     gender,
# MAGIC     tier,
# MAGIC     dob,
# MAGIC     address_line1,
# MAGIC     address_line2,
# MAGIC     postal_code,
# MAGIC     city,
# MAGIC     state_prov,
# MAGIC     country,
# MAGIC     email1,
# MAGIC     email2,
# MAGIC     local_tax_id,
# MAGIC     national_tax_id,
# MAGIC     CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,  -- D = inactive
# MAGIC     cdc_dsn AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     batch_id,
# MAGIC     load_timestamp,
# MAGIC     cdc_flag AS record_type
# MAGIC FROM incoming_customers
# MAGIC WHERE cdc_flag IN ('I', 'U');  -- Insert new and updated versions (not D-only)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_accounts: Parse Account.txt with SCD Type 2 MERGE
# MAGIC -- Format: CDC_FLAG|CDC_DSN|CA_ID|CA_B_ID|CA_C_ID|CA_NAME|CA_TAX_ST|CA_ST_ID
# MAGIC WITH incoming_accounts AS (
# MAGIC     SELECT 
# MAGIC         CAST(split(raw_line, '\\|')[2] AS BIGINT) AS account_id,
# MAGIC         CAST(split(raw_line, '\\|')[3] AS BIGINT) AS broker_id,
# MAGIC         CAST(split(raw_line, '\\|')[4] AS BIGINT) AS customer_id,
# MAGIC         split(raw_line, '\\|')[5] AS account_name,
# MAGIC         CAST(split(raw_line, '\\|')[6] AS INT) AS tax_status,
# MAGIC         split(raw_line, '\\|')[7] AS status_id,
# MAGIC         split(raw_line, '\\|')[0] AS cdc_flag,
# MAGIC         CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS cdc_dsn,
# MAGIC         ${var.batch_id} AS batch_id,
# MAGIC         current_timestamp() AS load_timestamp
# MAGIC     FROM bronze_account
# MAGIC     WHERE _batch_id = ${var.batch_id}
# MAGIC       AND raw_line IS NOT NULL
# MAGIC       AND raw_line != ''
# MAGIC       AND size(split(raw_line, '\\|')) >= 8
# MAGIC ),
# MAGIC updates_to_close AS (
# MAGIC     SELECT 
# MAGIC         account_id,
# MAGIC         MIN(cdc_dsn) AS new_effective_date
# MAGIC     FROM incoming_accounts
# MAGIC     WHERE cdc_flag IN ('U', 'D')
# MAGIC     GROUP BY account_id
# MAGIC )
# MAGIC MERGE INTO silver_accounts AS target
# MAGIC USING updates_to_close AS updates
# MAGIC ON target.account_id = updates.account_id 
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.end_date = updates.new_effective_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver_accounts
# MAGIC SELECT 
# MAGIC     account_id,
# MAGIC     broker_id,
# MAGIC     customer_id,
# MAGIC     account_name,
# MAGIC     tax_status,
# MAGIC     status_id,
# MAGIC     NULL AS action_type,
# MAGIC     cdc_dsn AS action_timestamp,
# MAGIC     CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
# MAGIC     cdc_dsn AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     batch_id,
# MAGIC     load_timestamp,
# MAGIC     cdc_flag AS record_type
# MAGIC FROM incoming_accounts
# MAGIC WHERE cdc_flag IN ('I', 'U');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Transaction Data: Incremental (with CDC columns)
# MAGIC -- ============================================================================
# MAGIC -- silver_trades: Parse Trade.txt (18 columns incremental: +CDC_FLAG, +CDC_DSN)
# MAGIC WITH incoming_trades AS (
# MAGIC     SELECT 
# MAGIC         CAST(split(raw_line, '\\|')[2] AS BIGINT) AS trade_id,  -- Skip CDC_FLAG, CDC_DSN
# MAGIC         CAST(split(raw_line, '\\|')[3] AS TIMESTAMP) AS trade_dts,
# MAGIC         split(raw_line, '\\|')[4] AS status_id,
# MAGIC         split(raw_line, '\\|')[5] AS trade_type_id,
# MAGIC         CAST(split(raw_line, '\\|')[6] AS BOOLEAN) AS is_cash,
# MAGIC         split(raw_line, '\\|')[7] AS symbol,
# MAGIC         CAST(split(raw_line, '\\|')[8] AS INT) AS quantity,
# MAGIC         CAST(split(raw_line, '\\|')[9] AS DOUBLE) AS bid_price,
# MAGIC         CAST(split(raw_line, '\\|')[10] AS BIGINT) AS account_id,
# MAGIC         split(raw_line, '\\|')[11] AS exec_name,
# MAGIC         CAST(split(raw_line, '\\|')[12] AS DOUBLE) AS trade_price,
# MAGIC         CAST(split(raw_line, '\\|')[13] AS DOUBLE) AS charge,
# MAGIC         CAST(split(raw_line, '\\|')[14] AS DOUBLE) AS commission,
# MAGIC         CAST(split(raw_line, '\\|')[15] AS DOUBLE) AS tax,
# MAGIC         split(raw_line, '\\|')[0] AS cdc_flag,
# MAGIC         CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS cdc_dsn,
# MAGIC         ${var.batch_id} AS batch_id,
# MAGIC         current_timestamp() AS load_timestamp
# MAGIC     FROM bronze_trade
# MAGIC     WHERE _batch_id = ${var.batch_id}
# MAGIC       AND raw_line IS NOT NULL
# MAGIC       AND raw_line != ''
# MAGIC       AND size(split(raw_line, '\\|')) = 18  -- Incremental = 18 columns
# MAGIC ),
# MAGIC updates_to_close AS (
# MAGIC     SELECT 
# MAGIC         trade_id,
# MAGIC         MIN(cdc_dsn) AS new_effective_date
# MAGIC     FROM incoming_trades
# MAGIC     WHERE cdc_flag IN ('U', 'D')
# MAGIC     GROUP BY trade_id
# MAGIC )
# MAGIC MERGE INTO silver_trades AS target
# MAGIC USING updates_to_close AS updates
# MAGIC ON target.trade_id = updates.trade_id 
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.end_date = updates.new_effective_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver_trades
# MAGIC SELECT 
# MAGIC     trade_id,
# MAGIC     trade_dts,
# MAGIC     status_id,
# MAGIC     trade_type_id,
# MAGIC     is_cash,
# MAGIC     symbol,
# MAGIC     quantity,
# MAGIC     bid_price,
# MAGIC     account_id,
# MAGIC     exec_name,
# MAGIC     trade_price,
# MAGIC     charge,
# MAGIC     commission,
# MAGIC     tax,
# MAGIC     CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
# MAGIC     cdc_dsn AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     batch_id,
# MAGIC     load_timestamp,
# MAGIC     cdc_flag AS record_type
# MAGIC FROM incoming_trades
# MAGIC WHERE cdc_flag IN ('I', 'U');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_daily_market: Parse DailyMarket.txt (8 columns incremental: +CDC_FLAG, +CDC_DSN)
# MAGIC MERGE INTO silver_daily_market AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         CONCAT(CAST(split(raw_line, '\\|')[2] AS DATE), '|', split(raw_line, '\\|')[3]) AS dm_key,
# MAGIC         CAST(split(raw_line, '\\|')[2] AS DATE) AS dm_date,
# MAGIC         split(raw_line, '\\|')[3] AS dm_s_symb,
# MAGIC         CAST(split(raw_line, '\\|')[4] AS DOUBLE) AS dm_close,
# MAGIC         CAST(split(raw_line, '\\|')[5] AS DOUBLE) AS dm_high,
# MAGIC         CAST(split(raw_line, '\\|')[6] AS DOUBLE) AS dm_low,
# MAGIC         CAST(split(raw_line, '\\|')[7] AS BIGINT) AS dm_vol,
# MAGIC         ${var.batch_id} AS batch_id,
# MAGIC         current_timestamp() AS load_timestamp
# MAGIC     FROM bronze_daily_market
# MAGIC     WHERE _batch_id = ${var.batch_id}
# MAGIC       AND raw_line IS NOT NULL
# MAGIC       AND raw_line != ''
# MAGIC       AND size(split(raw_line, '\\|')) = 8  -- Incremental = 8 columns
# MAGIC ) AS source
# MAGIC ON target.dm_key = source.dm_key
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.dm_close = source.dm_close,
# MAGIC     target.dm_high = source.dm_high,
# MAGIC     target.dm_low = source.dm_low,
# MAGIC     target.dm_vol = source.dm_vol,
# MAGIC     target.batch_id = source.batch_id,
# MAGIC     target.load_timestamp = source.load_timestamp
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_cash_transaction: Parse CashTransaction.txt (6 columns incremental)
# MAGIC WITH incoming_cash AS (
# MAGIC     SELECT 
# MAGIC         CONCAT(CAST(split(raw_line, '\\|')[2] AS BIGINT), '|', CAST(split(raw_line, '\\|')[3] AS TIMESTAMP)) AS ct_key,
# MAGIC         CAST(split(raw_line, '\\|')[2] AS BIGINT) AS ct_ca_id,
# MAGIC         CAST(split(raw_line, '\\|')[3] AS TIMESTAMP) AS ct_dts,
# MAGIC         CAST(split(raw_line, '\\|')[4] AS DOUBLE) AS ct_amt,
# MAGIC         split(raw_line, '\\|')[5] AS ct_name,
# MAGIC         split(raw_line, '\\|')[0] AS cdc_flag,
# MAGIC         CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS cdc_dsn,
# MAGIC         ${var.batch_id} AS batch_id,
# MAGIC         current_timestamp() AS load_timestamp
# MAGIC     FROM bronze_cash_transaction
# MAGIC     WHERE _batch_id = ${var.batch_id}
# MAGIC       AND raw_line IS NOT NULL
# MAGIC       AND raw_line != ''
# MAGIC       AND size(split(raw_line, '\\|')) = 6
# MAGIC ),
# MAGIC updates_to_close AS (
# MAGIC     SELECT 
# MAGIC         ct_key,
# MAGIC         MIN(cdc_dsn) AS new_effective_date
# MAGIC     FROM incoming_cash
# MAGIC     WHERE cdc_flag IN ('U', 'D')
# MAGIC     GROUP BY ct_key
# MAGIC )
# MAGIC MERGE INTO silver_cash_transaction AS target
# MAGIC USING updates_to_close AS updates
# MAGIC ON target.ct_key = updates.ct_key 
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.end_date = updates.new_effective_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver_cash_transaction
# MAGIC SELECT 
# MAGIC     ct_key,
# MAGIC     ct_ca_id,
# MAGIC     ct_dts,
# MAGIC     ct_amt,
# MAGIC     ct_name,
# MAGIC     CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
# MAGIC     cdc_dsn AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     batch_id,
# MAGIC     load_timestamp,
# MAGIC     cdc_flag AS record_type
# MAGIC FROM incoming_cash
# MAGIC WHERE cdc_flag IN ('I', 'U');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_holding_history: Parse HoldingHistory.txt (6 columns incremental)
# MAGIC WITH incoming_holdings AS (
# MAGIC     SELECT 
# MAGIC         CAST(split(raw_line, '\\|')[2] AS BIGINT) AS hh_h_t_id,
# MAGIC         CAST(split(raw_line, '\\|')[3] AS BIGINT) AS hh_t_id,
# MAGIC         CAST(split(raw_line, '\\|')[4] AS INT) AS hh_before_qty,
# MAGIC         CAST(split(raw_line, '\\|')[5] AS INT) AS hh_after_qty,
# MAGIC         split(raw_line, '\\|')[0] AS cdc_flag,
# MAGIC         CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS cdc_dsn,
# MAGIC         ${var.batch_id} AS batch_id,
# MAGIC         current_timestamp() AS load_timestamp
# MAGIC     FROM bronze_holding_history
# MAGIC     WHERE _batch_id = ${var.batch_id}
# MAGIC       AND raw_line IS NOT NULL
# MAGIC       AND raw_line != ''
# MAGIC       AND size(split(raw_line, '\\|')) = 6
# MAGIC ),
# MAGIC updates_to_close AS (
# MAGIC     SELECT 
# MAGIC         hh_h_t_id,
# MAGIC         MIN(cdc_dsn) AS new_effective_date
# MAGIC     FROM incoming_holdings
# MAGIC     WHERE cdc_flag IN ('U', 'D')
# MAGIC     GROUP BY hh_h_t_id
# MAGIC )
# MAGIC MERGE INTO silver_holding_history AS target
# MAGIC USING updates_to_close AS updates
# MAGIC ON target.hh_h_t_id = updates.hh_h_t_id 
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.end_date = updates.new_effective_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver_holding_history
# MAGIC SELECT 
# MAGIC     hh_h_t_id,
# MAGIC     hh_t_id,
# MAGIC     hh_before_qty,
# MAGIC     hh_after_qty,
# MAGIC     CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
# MAGIC     cdc_dsn AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     batch_id,
# MAGIC     load_timestamp,
# MAGIC     cdc_flag AS record_type
# MAGIC FROM incoming_holdings
# MAGIC WHERE cdc_flag IN ('I', 'U');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_watch_history: Parse WatchHistory.txt (6 columns incremental)
# MAGIC WITH incoming_watches AS (
# MAGIC     SELECT 
# MAGIC         CONCAT(CAST(split(raw_line, '\\|')[2] AS BIGINT), '|', split(raw_line, '\\|')[3]) AS wh_key,
# MAGIC         CAST(split(raw_line, '\\|')[2] AS BIGINT) AS w_c_id,
# MAGIC         split(raw_line, '\\|')[3] AS w_s_symb,
# MAGIC         CAST(split(raw_line, '\\|')[4] AS TIMESTAMP) AS w_dts,
# MAGIC         split(raw_line, '\\|')[5] AS w_action,
# MAGIC         split(raw_line, '\\|')[0] AS cdc_flag,
# MAGIC         CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS cdc_dsn,
# MAGIC         ${var.batch_id} AS batch_id,
# MAGIC         current_timestamp() AS load_timestamp
# MAGIC     FROM bronze_watch_history
# MAGIC     WHERE _batch_id = ${var.batch_id}
# MAGIC       AND raw_line IS NOT NULL
# MAGIC       AND raw_line != ''
# MAGIC       AND size(split(raw_line, '\\|')) = 6
# MAGIC ),
# MAGIC updates_to_close AS (
# MAGIC     SELECT 
# MAGIC         wh_key,
# MAGIC         MIN(cdc_dsn) AS new_effective_date
# MAGIC     FROM incoming_watches
# MAGIC     WHERE cdc_flag IN ('U', 'D')
# MAGIC     GROUP BY wh_key
# MAGIC )
# MAGIC MERGE INTO silver_watch_history AS target
# MAGIC USING updates_to_close AS updates
# MAGIC ON target.wh_key = updates.wh_key 
# MAGIC    AND target.is_current = true
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.is_current = false,
# MAGIC     target.end_date = updates.new_effective_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO silver_watch_history
# MAGIC SELECT 
# MAGIC     wh_key,
# MAGIC     w_c_id,
# MAGIC     w_s_symb,
# MAGIC     w_dts,
# MAGIC     w_action,
# MAGIC     CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
# MAGIC     cdc_dsn AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     batch_id,
# MAGIC     load_timestamp,
# MAGIC     cdc_flag AS record_type
# MAGIC FROM incoming_watches
# MAGIC WHERE cdc_flag IN ('I', 'U');

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