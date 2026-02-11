# Databricks notebook source
# MAGIC %md
# MAGIC # Transform Bronze to Silver (Batch 1)
# MAGIC
# MAGIC Transforms Bronze data to Silver layer

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog")
dbutils.widgets.text("schema_name", "tpcdi_schema_sf10", "Schema Name")
dbutils.widgets.text("raw_data_path", "gs://sumit_prakash_gcs/tpcdi", "Raw Data Path")
dbutils.widgets.text("sf", "10", "Scale Factor")
dbutils.widgets.text("batch_id", "1", "Batch ID")

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

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- TPC-DI v2: Silver Layer - Batch 1 Transformations
# MAGIC -- ============================================================================
# MAGIC -- Transforms Bronze raw data into Silver cleaned, typed tables
# MAGIC -- Batch 1: Historical load (overwrite mode)
# MAGIC -- ============================================================================
# MAGIC -- Set variables
# MAGIC -- SET var.batch_id = 1;
# MAGIC -- ============================================================================
# MAGIC -- Reference Data (Batch 1: Overwrite)
# MAGIC -- ============================================================================
# COMMAND ----------

# Set catalog and create/use schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_date: Parse Date.txt (18 columns pipe-delimited)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver_date AS
# MAGIC SELECT 
# MAGIC     CAST(split(raw_line, '\\|')[0] AS INT) AS sk_date_id,
# MAGIC     CAST(split(raw_line, '\\|')[1] AS DATE) AS date_value,
# MAGIC     split(raw_line, '\\|')[2] AS date_desc,
# MAGIC     CAST(split(raw_line, '\\|')[3] AS INT) AS calendar_year_id,
# MAGIC     split(raw_line, '\\|')[4] AS calendar_year_desc,
# MAGIC     CAST(split(raw_line, '\\|')[5] AS INT) AS calendar_qtr_id,
# MAGIC     split(raw_line, '\\|')[6] AS calendar_qtr_desc,
# MAGIC     CAST(split(raw_line, '\\|')[7] AS INT) AS calendar_month_id,
# MAGIC     split(raw_line, '\\|')[8] AS calendar_month_desc,
# MAGIC     CAST(split(raw_line, '\\|')[9] AS INT) AS calendar_week_id,
# MAGIC     split(raw_line, '\\|')[10] AS calendar_week_desc,
# MAGIC     CAST(split(raw_line, '\\|')[11] AS INT) AS day_of_week_num,
# MAGIC     split(raw_line, '\\|')[12] AS day_of_week_desc,
# MAGIC     CAST(split(raw_line, '\\|')[13] AS INT) AS fiscal_year_id,
# MAGIC     split(raw_line, '\\|')[14] AS fiscal_year_desc,
# MAGIC     CAST(split(raw_line, '\\|')[15] AS INT) AS fiscal_qtr_id,
# MAGIC     split(raw_line, '\\|')[16] AS fiscal_qtr_desc,
# MAGIC     CAST(split(raw_line, '\\|')[17] AS BOOLEAN) AS holiday_flag,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC FROM bronze_date
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_time: Parse Time.txt (10 columns pipe-delimited)
# MAGIC CREATE OR REPLACE TABLE silver_time AS
# MAGIC SELECT 
# MAGIC     CAST(split(raw_line, '\\|')[0] AS INT) AS sk_time_id,
# MAGIC     split(raw_line, '\\|')[1] AS time_value,  -- STRING (Spark has no TIME type)
# MAGIC     CAST(split(raw_line, '\\|')[2] AS INT) AS hour_id,
# MAGIC     split(raw_line, '\\|')[3] AS hour_desc,
# MAGIC     CAST(split(raw_line, '\\|')[4] AS INT) AS minute_id,
# MAGIC     split(raw_line, '\\|')[5] AS minute_desc,
# MAGIC     CAST(split(raw_line, '\\|')[6] AS INT) AS second_id,
# MAGIC     split(raw_line, '\\|')[7] AS second_desc,
# MAGIC     CAST(split(raw_line, '\\|')[8] AS BOOLEAN) AS market_hours_flag,
# MAGIC     CAST(split(raw_line, '\\|')[9] AS BOOLEAN) AS office_hours_flag,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC FROM bronze_time
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_status_type: Parse StatusType.txt (2 columns)
# MAGIC CREATE OR REPLACE TABLE silver_status_type AS
# MAGIC SELECT 
# MAGIC     split(raw_line, '\\|')[0] AS st_id,
# MAGIC     split(raw_line, '\\|')[1] AS st_name,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC FROM bronze_status_type
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_trade_type: Parse TradeType.txt (4 columns)
# MAGIC CREATE OR REPLACE TABLE silver_trade_type AS
# MAGIC SELECT 
# MAGIC     split(raw_line, '\\|')[0] AS tt_id,
# MAGIC     split(raw_line, '\\|')[1] AS tt_name,
# MAGIC     CAST(split(raw_line, '\\|')[2] AS BOOLEAN) AS tt_is_sell,
# MAGIC     CAST(split(raw_line, '\\|')[3] AS BOOLEAN) AS tt_is_mrkt,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC FROM bronze_trade_type
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_industry: Parse Industry.txt (3 columns)
# MAGIC CREATE OR REPLACE TABLE silver_industry AS
# MAGIC SELECT 
# MAGIC     split(raw_line, '\\|')[0] AS in_id,
# MAGIC     split(raw_line, '\\|')[1] AS in_name,
# MAGIC     split(raw_line, '\\|')[2] AS in_sc_id,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC FROM bronze_industry
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != '';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_tax_rate: Parse TaxRate.txt (3 columns)
# MAGIC CREATE OR REPLACE TABLE silver_tax_rate AS
# MAGIC SELECT 
# MAGIC     split(raw_line, '\\|')[0] AS tx_id,
# MAGIC     split(raw_line, '\\|')[1] AS tx_name,
# MAGIC     CAST(split(raw_line, '\\|')[2] AS DOUBLE) AS tx_rate,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC FROM bronze_tax_rate
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != '';

# COMMAND ----------

# ============================================================================
# Market Data: Parse FINWIRE (Fixed-Width) - silver_companies (CMP records)
# ============================================================================
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_companies AS
SELECT 
    monotonically_increasing_id() AS sk_company_id,
    TRIM(substring(raw_line, 79, 10)) AS company_id,
    TRIM(substring(raw_line, 19, 60)) AS company_name,
    TRIM(substring(raw_line, 93, 2)) AS industry_id,
    TRIM(substring(raw_line, 95, 4)) AS sp_rating,
    TRIM(substring(raw_line, 89, 4)) AS status,
    CAST(TRIM(substring(raw_line, 99, 8)) AS DATE) AS founding_date,
    TRIM(substring(raw_line, 348, 46)) AS ceo_name,
    TRIM(substring(raw_line, 107, 80)) AS address_line1,
    TRIM(substring(raw_line, 187, 80)) AS address_line2,
    TRIM(substring(raw_line, 267, 12)) AS postal_code,
    TRIM(substring(raw_line, 279, 25)) AS city,
    TRIM(substring(raw_line, 304, 20)) AS state_province,
    TRIM(substring(raw_line, 324, 24)) AS country,
    TRIM(substring(raw_line, 394, 150)) AS description,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_finwire
WHERE _batch_id = {batch_id}
  AND substring(raw_line, 16, 3) = 'CMP'
  AND length(raw_line) >= 394
""")

# COMMAND ----------

# silver_securities: Extract SEC records from FINWIRE (fixed-width positions)
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_securities AS
SELECT 
    TRIM(substring(raw_line, 19, 15)) AS symbol,
    TRIM(substring(raw_line, 34, 6)) AS issue_type,
    TRIM(substring(raw_line, 40, 4)) AS status,
    TRIM(substring(raw_line, 44, 70)) AS name,
    TRIM(substring(raw_line, 114, 6)) AS ex_id,
    CAST(TRIM(substring(raw_line, 120, 13)) AS BIGINT) AS sh_out,
    CAST(TRIM(substring(raw_line, 133, 8)) AS DATE) AS first_trade_date,
    TRIM(substring(raw_line, 141, 8)) AS first_trade_exchg,
    CAST(TRIM(substring(raw_line, 149, 12)) AS DOUBLE) AS dividend,
    TRIM(substring(raw_line, 161, 60)) AS co_name_or_cik,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_finwire
WHERE _batch_id = {batch_id}
  AND substring(raw_line, 16, 3) = 'SEC'
  AND length(raw_line) >= 220
""")

# COMMAND ----------

# silver_financials: Extract FIN records from FINWIRE (fixed-width per TPC-DI FIN layout)
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_name}.silver_financials AS
SELECT 
    TRIM(substring(raw_line, 187, 60)) AS co_name_or_cik,
    CAST(TRIM(substring(raw_line, 19, 4)) AS INT) AS year,
    CAST(TRIM(substring(raw_line, 23, 1)) AS INT) AS quarter,
    CAST(TRIM(substring(raw_line, 24, 8)) AS DATE) AS qtr_start_date,
    CAST(TRIM(substring(raw_line, 32, 8)) AS DATE) AS posting_date,
    CAST(TRIM(substring(raw_line, 40, 17)) AS DOUBLE) AS revenue,
    CAST(TRIM(substring(raw_line, 57, 17)) AS DOUBLE) AS earnings,
    CAST(TRIM(substring(raw_line, 74, 12)) AS DOUBLE) AS eps,
    CAST(TRIM(substring(raw_line, 86, 12)) AS DOUBLE) AS diluted_eps,
    CAST(TRIM(substring(raw_line, 98, 12)) AS DOUBLE) AS margin,
    CAST(TRIM(substring(raw_line, 110, 17)) AS DOUBLE) AS inventory,
    CAST(TRIM(substring(raw_line, 127, 17)) AS DOUBLE) AS assets,
    CAST(TRIM(substring(raw_line, 144, 17)) AS DOUBLE) AS liabilities,
    CAST(TRIM(substring(raw_line, 161, 13)) AS BIGINT) AS sh_out,
    CAST(TRIM(substring(raw_line, 174, 13)) AS BIGINT) AS diluted_sh_out,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM {catalog}.{schema_name}.bronze_finwire
WHERE _batch_id = {batch_id}
  AND substring(raw_line, 16, 3) = 'FIN'
  AND length(raw_line) >= 246
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Brokerage Data: Parse CustomerMgmt.xml (Batch 1)
# MAGIC -- ============================================================================
# MAGIC -- silver_customers: Extract from CustomerMgmt.xml
# MAGIC -- Note: This assumes XML is parsed using spark-xml or native XML reader
# MAGIC -- Adjust column paths based on your XML parsing method
# MAGIC CREATE OR REPLACE TABLE silver_customers AS
# MAGIC SELECT 
# MAGIC     monotonically_increasing_id() AS sk_customer_id,
# MAGIC     CAST(Customer._C_ID AS BIGINT) AS customer_id,
# MAGIC     Customer._C_TAX_ID AS tax_id,
# MAGIC     Customer._C_ST_ID AS status,
# MAGIC     Customer._C_L_NAME AS last_name,
# MAGIC     Customer._C_F_NAME AS first_name,
# MAGIC     Customer._C_M_NAME AS middle_name,
# MAGIC     Customer._C_GNDR AS gender,
# MAGIC     CAST(Customer._C_TIER AS INT) AS tier,
# MAGIC     CAST(Customer._C_DOB AS DATE) AS dob,
# MAGIC     Customer._C_ADLINE1 AS address_line1,
# MAGIC     Customer._C_ADLINE2 AS address_line2,
# MAGIC     Customer._C_ZIPCODE AS postal_code,
# MAGIC     Customer._C_CITY AS city,
# MAGIC     Customer._C_STATE_PROV AS state_prov,
# MAGIC     Customer._C_CTRY AS country,
# MAGIC     Customer._C_CTRY_1 AS email1,
# MAGIC     Customer._C_CTRY_2 AS email2,
# MAGIC     Customer._C_LOCAL_TAX_ID AS local_tax_id,
# MAGIC     Customer._C_NAT_TX_ID AS national_tax_id,
# MAGIC     -- SCD Type 2: All Batch 1 records are current
# MAGIC     TRUE AS is_current,
# MAGIC     CAST(Customer._C_CTRY_TS AS TIMESTAMP) AS effective_date,  -- Use action timestamp
# MAGIC     NULL AS end_date,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp,
# MAGIC     Customer._C_ACTION AS record_type  -- NEW, UPDCUST, INACT, etc.
# MAGIC FROM bronze_customer_mgmt
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND Customer IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_accounts: Extract from CustomerMgmt.xml
# MAGIC CREATE OR REPLACE TABLE silver_accounts AS
# MAGIC SELECT 
# MAGIC     CAST(Account._CA_ID AS BIGINT) AS account_id,
# MAGIC     CAST(Account._CA_B_ID AS BIGINT) AS broker_id,
# MAGIC     CAST(Account._CA_C_ID AS BIGINT) AS customer_id,
# MAGIC     Account._CA_NAME AS account_name,
# MAGIC     CAST(Account._CA_TAX_ST AS INT) AS tax_status,
# MAGIC     Account._CA_ST_ID AS status_id,
# MAGIC     Account._CA_ACTION AS action_type,
# MAGIC     CAST(Account._CA_ACTION_TS AS TIMESTAMP) AS action_timestamp,
# MAGIC     -- SCD Type 2: All Batch 1 records are current
# MAGIC     TRUE AS is_current,
# MAGIC     CAST(Account._CA_ACTION_TS AS TIMESTAMP) AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp,
# MAGIC     Account._CA_ACTION AS record_type  -- NEW, ADDACCT, UPDACCT, CLOSEACCT, etc.
# MAGIC FROM bronze_customer_mgmt
# MAGIC LATERAL VIEW explode(Customer.Account) AS Account
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND Customer IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Transaction Data (Batch 1)
# MAGIC -- ============================================================================
# MAGIC -- silver_trades: Parse Trade.txt (16 columns historical)
# MAGIC CREATE OR REPLACE TABLE silver_trades AS
# MAGIC SELECT 
# MAGIC     CAST(split(raw_line, '\\|')[0] AS BIGINT) AS trade_id,
# MAGIC     CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS trade_dts,
# MAGIC     split(raw_line, '\\|')[2] AS status_id,
# MAGIC     split(raw_line, '\\|')[3] AS trade_type_id,
# MAGIC     CAST(split(raw_line, '\\|')[4] AS BOOLEAN) AS is_cash,
# MAGIC     split(raw_line, '\\|')[5] AS symbol,
# MAGIC     CAST(split(raw_line, '\\|')[6] AS INT) AS quantity,
# MAGIC     CAST(split(raw_line, '\\|')[7] AS DOUBLE) AS bid_price,
# MAGIC     CAST(split(raw_line, '\\|')[8] AS BIGINT) AS account_id,
# MAGIC     split(raw_line, '\\|')[9] AS exec_name,
# MAGIC     CAST(split(raw_line, '\\|')[10] AS DOUBLE) AS trade_price,
# MAGIC     CAST(split(raw_line, '\\|')[11] AS DOUBLE) AS charge,
# MAGIC     CAST(split(raw_line, '\\|')[12] AS DOUBLE) AS commission,
# MAGIC     CAST(split(raw_line, '\\|')[13] AS DOUBLE) AS tax,
# MAGIC     -- SCD Type 2: All Batch 1 records are current
# MAGIC     TRUE AS is_current,
# MAGIC     CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS effective_date,  -- Use trade_dts
# MAGIC     NULL AS end_date,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp,
# MAGIC     NULL AS record_type  -- Historical has no record_type
# MAGIC FROM bronze_trade
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != ''
# MAGIC   AND size(split(raw_line, '\\|')) = 16;  -- Historical = 16 columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_daily_market: Parse DailyMarket.txt (6 columns historical)
# MAGIC CREATE OR REPLACE TABLE silver_daily_market AS
# MAGIC SELECT 
# MAGIC     CONCAT(CAST(split(raw_line, '\\|')[0] AS DATE), '|', split(raw_line, '\\|')[1]) AS dm_key,
# MAGIC     CAST(split(raw_line, '\\|')[0] AS DATE) AS dm_date,
# MAGIC     split(raw_line, '\\|')[1] AS dm_s_symb,
# MAGIC     CAST(split(raw_line, '\\|')[2] AS DOUBLE) AS dm_close,
# MAGIC     CAST(split(raw_line, '\\|')[3] AS DOUBLE) AS dm_high,
# MAGIC     CAST(split(raw_line, '\\|')[4] AS DOUBLE) AS dm_low,
# MAGIC     CAST(split(raw_line, '\\|')[5] AS BIGINT) AS dm_vol,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp
# MAGIC FROM bronze_daily_market
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != ''
# MAGIC   AND size(split(raw_line, '\\|')) = 6;  -- Historical = 6 columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_cash_transaction: Parse CashTransaction.txt (4 columns historical)
# MAGIC CREATE OR REPLACE TABLE silver_cash_transaction AS
# MAGIC SELECT 
# MAGIC     CONCAT(CAST(split(raw_line, '\\|')[0] AS BIGINT), '|', CAST(split(raw_line, '\\|')[1] AS TIMESTAMP)) AS ct_key,
# MAGIC     CAST(split(raw_line, '\\|')[0] AS BIGINT) AS ct_ca_id,
# MAGIC     CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS ct_dts,
# MAGIC     CAST(split(raw_line, '\\|')[2] AS DOUBLE) AS ct_amt,
# MAGIC     split(raw_line, '\\|')[3] AS ct_name,
# MAGIC     TRUE AS is_current,
# MAGIC     CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp,
# MAGIC     NULL AS record_type
# MAGIC FROM bronze_cash_transaction
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != ''
# MAGIC   AND size(split(raw_line, '\\|')) = 4;  -- Historical = 4 columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_holding_history: Parse HoldingHistory.txt (4 columns historical)
# MAGIC CREATE OR REPLACE TABLE silver_holding_history AS
# MAGIC SELECT 
# MAGIC     CAST(split(raw_line, '\\|')[0] AS BIGINT) AS hh_h_t_id,
# MAGIC     CAST(split(raw_line, '\\|')[1] AS BIGINT) AS hh_t_id,
# MAGIC     CAST(split(raw_line, '\\|')[2] AS INT) AS hh_before_qty,
# MAGIC     CAST(split(raw_line, '\\|')[3] AS INT) AS hh_after_qty,
# MAGIC     TRUE AS is_current,
# MAGIC     current_timestamp() AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp,
# MAGIC     NULL AS record_type
# MAGIC FROM bronze_holding_history
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != ''
# MAGIC   AND size(split(raw_line, '\\|')) = 4;  -- Historical = 4 columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- silver_watch_history: Parse WatchHistory.txt (4 columns historical)
# MAGIC CREATE OR REPLACE TABLE silver_watch_history AS
# MAGIC SELECT 
# MAGIC     CONCAT(CAST(split(raw_line, '\\|')[0] AS BIGINT), '|', split(raw_line, '\\|')[1]) AS wh_key,
# MAGIC     CAST(split(raw_line, '\\|')[0] AS BIGINT) AS w_c_id,
# MAGIC     split(raw_line, '\\|')[1] AS w_s_symb,
# MAGIC     CAST(split(raw_line, '\\|')[2] AS TIMESTAMP) AS w_dts,
# MAGIC     split(raw_line, '\\|')[3] AS w_action,
# MAGIC     TRUE AS is_current,
# MAGIC     CAST(split(raw_line, '\\|')[2] AS TIMESTAMP) AS effective_date,
# MAGIC     NULL AS end_date,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     current_timestamp() AS load_timestamp,
# MAGIC     NULL AS record_type
# MAGIC FROM bronze_watch_history
# MAGIC WHERE _batch_id = ${var.batch_id}
# MAGIC   AND raw_line IS NOT NULL
# MAGIC   AND raw_line != ''
# MAGIC   AND size(split(raw_line, '\\|')) = 4;  -- Historical = 4 columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Other Sources (Batch 1)
# MAGIC -- ============================================================================
# MAGIC -- silver_prospect: Parse Prospect.csv (23 columns comma-delimited)
# MAGIC CREATE OR REPLACE TABLE silver_prospect AS
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