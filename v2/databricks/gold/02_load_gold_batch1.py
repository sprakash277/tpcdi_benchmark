# Databricks notebook source
# MAGIC %md
# MAGIC # Load Gold Batch 1 Data
# MAGIC
# MAGIC Loads Silver data into Gold tables

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
# MAGIC -- TPC-DI v2: Gold Layer - Batch 1 Load (Historical)
# MAGIC -- ============================================================================
# MAGIC -- Loads Silver data into Gold star schema tables
# MAGIC -- Batch 1: Bulk INSERT (no MERGE needed)
# MAGIC -- ============================================================================
# MAGIC -- Set variables
# MAGIC -- SET var.batch_id = 1;
# MAGIC -- ============================================================================
# MAGIC -- Dimension Tables (Batch 1: INSERT)
# MAGIC -- ============================================================================
# COMMAND ----------

# Set catalog and create/use schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_customer: Current versions only from silver_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_dim_customer AS
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
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_customers
# MAGIC WHERE is_current = true
# MAGIC   AND batch_id = ${var.batch_id}
# MAGIC   AND customer_id != -1;  -- Exclude placeholder

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_account: Current versions only from silver_accounts
# MAGIC CREATE OR REPLACE TABLE gold_dim_account AS
# MAGIC SELECT 
# MAGIC     monotonically_increasing_id() AS sk_account_id,
# MAGIC     account_id,
# MAGIC     broker_id,
# MAGIC     customer_id,
# MAGIC     account_name,
# MAGIC     tax_status,
# MAGIC     status_id,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_accounts
# MAGIC WHERE is_current = true
# MAGIC   AND batch_id = ${var.batch_id}
# MAGIC   AND account_id != -1;  -- Exclude placeholder

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_date: From silver_date
# MAGIC CREATE OR REPLACE TABLE gold_dim_date AS
# MAGIC SELECT 
# MAGIC     sk_date_id AS sk_date_id,
# MAGIC     sk_date_id AS date_id,
# MAGIC     date_value,
# MAGIC     date_desc,
# MAGIC     calendar_year_id,
# MAGIC     calendar_year_desc,
# MAGIC     calendar_qtr_id,
# MAGIC     calendar_qtr_desc,
# MAGIC     calendar_month_id,
# MAGIC     calendar_month_desc,
# MAGIC     calendar_week_id,
# MAGIC     calendar_week_desc,
# MAGIC     day_of_week_num,
# MAGIC     day_of_week_desc,
# MAGIC     fiscal_year_id,
# MAGIC     fiscal_year_desc,
# MAGIC     fiscal_qtr_id,
# MAGIC     fiscal_qtr_desc,
# MAGIC     holiday_flag,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_date
# MAGIC WHERE batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_time: From silver_time
# MAGIC CREATE OR REPLACE TABLE gold_dim_time AS
# MAGIC SELECT 
# MAGIC     sk_time_id AS sk_time_id,
# MAGIC     sk_time_id AS time_id,
# MAGIC     time_value,
# MAGIC     hour_id,
# MAGIC     hour_desc,
# MAGIC     minute_id,
# MAGIC     minute_desc,
# MAGIC     second_id,
# MAGIC     second_desc,
# MAGIC     market_hours_flag,
# MAGIC     office_hours_flag,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_time
# MAGIC WHERE batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_trade_type: From silver_trade_type
# MAGIC CREATE OR REPLACE TABLE gold_dim_trade_type AS
# MAGIC SELECT 
# MAGIC     tt_id AS sk_trade_type_id,
# MAGIC     tt_id AS trade_type_id,
# MAGIC     tt_id AS trade_type_code,
# MAGIC     tt_name AS trade_type_name,
# MAGIC     tt_is_sell AS is_sell,
# MAGIC     tt_is_mrkt AS is_market,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_trade_type
# MAGIC WHERE batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_status_type: From silver_status_type
# MAGIC CREATE OR REPLACE TABLE gold_dim_status_type AS
# MAGIC SELECT 
# MAGIC     st_id AS sk_status_type_id,
# MAGIC     st_id AS status_type_id,
# MAGIC     st_id AS status_type_code,
# MAGIC     st_name AS status_type_name,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_status_type
# MAGIC WHERE batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_industry: From silver_industry
# MAGIC CREATE OR REPLACE TABLE gold_dim_industry AS
# MAGIC SELECT 
# MAGIC     in_id AS sk_industry_id,
# MAGIC     in_id AS industry_id,
# MAGIC     in_name AS industry_name,
# MAGIC     in_sc_id AS sector_id,
# MAGIC     NULL AS sector_name,  -- Lookup or derive if needed
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_industry
# MAGIC WHERE batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_company: From silver_companies (current only)
# MAGIC CREATE OR REPLACE TABLE gold_dim_company AS
# MAGIC SELECT 
# MAGIC     sc.sk_company_id,
# MAGIC     sc.company_id,
# MAGIC     sc.company_name,
# MAGIC     sc.industry_id,
# MAGIC     si.in_sc_id AS sector,  -- Join to industry for sector
# MAGIC     sc.status,
# MAGIC     sc.address_line1,
# MAGIC     sc.address_line2,
# MAGIC     sc.postal_code,
# MAGIC     sc.city,
# MAGIC     sc.state_province AS state_prov,
# MAGIC     sc.country,
# MAGIC     sc.description,
# MAGIC     sc.founding_date,
# MAGIC     sc.ceo_name,
# MAGIC     TRUE AS is_current,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_companies sc
# MAGIC LEFT JOIN silver_industry si ON sc.industry_id = si.in_id
# MAGIC WHERE sc.batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_security: From silver_securities (current only)
# MAGIC CREATE OR REPLACE TABLE gold_dim_security AS
# MAGIC SELECT 
# MAGIC     ss.symbol AS sk_security_id,
# MAGIC     ss.symbol AS security_id,
# MAGIC     ss.symbol,
# MAGIC     ss.issue_type,
# MAGIC     ss.status,
# MAGIC     ss.name,
# MAGIC     ss.ex_id AS exchange_id,
# MAGIC     ss.sh_out AS shares_outstanding,
# MAGIC     ss.first_trade_date,
# MAGIC     ss.first_trade_exchg AS first_trade_exchange,
# MAGIC     ss.dividend,
# MAGIC     ss.co_name_or_cik AS company_id,  -- Reference to DimCompany
# MAGIC     TRUE AS is_current,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_securities ss
# MAGIC WHERE ss.batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_broker: From silver_hr (extract brokers)
# MAGIC -- Note: This assumes HR.csv has been parsed and brokers identified
# MAGIC -- Adjust based on your HR parsing logic
# MAGIC CREATE OR REPLACE TABLE gold_dim_broker AS
# MAGIC SELECT 
# MAGIC     monotonically_increasing_id() AS sk_broker_id,
# MAGIC     CAST(employee_id AS BIGINT) AS broker_id,
# MAGIC     CONCAT(first_name, ' ', last_name) AS broker_name,
# MAGIC     branch AS branch,
# MAGIC     office AS office,
# MAGIC     phone AS phone,
# MAGIC     TRUE AS is_current,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM (
# MAGIC     -- Parse HR.csv: EmployeeID|ManagerID|EmployeeFirstName|EmployeeLastName|...
# MAGIC     -- Filter where job code indicates broker
# MAGIC     SELECT DISTINCT
# MAGIC         split(raw_line, ',')[0] AS employee_id,
# MAGIC         split(raw_line, ',')[1] AS manager_id,
# MAGIC         split(raw_line, ',')[2] AS first_name,
# MAGIC         split(raw_line, ',')[3] AS last_name,
# MAGIC         split(raw_line, ',')[4] AS branch,
# MAGIC         split(raw_line, ',')[5] AS office,
# MAGIC         split(raw_line, ',')[6] AS phone,
# MAGIC         split(raw_line, ',')[7] AS job_code
# MAGIC     FROM bronze_hr
# MAGIC     WHERE _batch_id = ${var.batch_id}
# MAGIC       AND raw_line IS NOT NULL
# MAGIC       AND split(raw_line, ',')[7] LIKE '%BROKER%'  -- Adjust filter as needed
# MAGIC ) AS brokers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Fact Tables (Batch 1: INSERT)
# MAGIC -- ============================================================================
# MAGIC -- gold_fact_trade: Join trades with dimensions
# MAGIC CREATE OR REPLACE TABLE gold_fact_trade AS
# MAGIC SELECT 
# MAGIC     st.trade_id AS sk_trade_id,  -- Use trade_id as surrogate key
# MAGIC     dd.sk_date_id,
# MAGIC     dt.sk_time_id,
# MAGIC     dc.sk_customer_id,
# MAGIC     da.sk_account_id,
# MAGIC     ds.sk_security_id,
# MAGIC     dtt.sk_trade_type_id,
# MAGIC     st.trade_id,
# MAGIC     st.trade_dts,
# MAGIC     st.trade_price,
# MAGIC     st.quantity AS trade_quantity,
# MAGIC     st.trade_price * st.quantity AS trade_amount,
# MAGIC     st.commission,
# MAGIC     st.charge,
# MAGIC     st.tax,
# MAGIC     st.status_id,
# MAGIC     st.is_cash,
# MAGIC     st.exec_name,
# MAGIC     st.batch_id,
# MAGIC     FALSE AS late_arriving_flag,  -- Batch 1 has no late arrivals
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_trades st
# MAGIC INNER JOIN gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
# MAGIC LEFT JOIN gold_dim_time dt ON HOUR(st.trade_dts) = dt.hour_id
# MAGIC INNER JOIN gold_dim_account da ON st.account_id = da.account_id
# MAGIC INNER JOIN gold_dim_customer dc ON da.customer_id = dc.customer_id
# MAGIC INNER JOIN gold_dim_security ds ON st.symbol = ds.symbol
# MAGIC INNER JOIN gold_dim_trade_type dtt ON st.trade_type_id = dtt.trade_type_id
# MAGIC WHERE st.batch_id = ${var.batch_id}
# MAGIC   AND st.is_current = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_fact_market_history: From silver_daily_market
# MAGIC CREATE OR REPLACE TABLE gold_fact_market_history AS
# MAGIC SELECT 
# MAGIC     dd.sk_date_id,
# MAGIC     ds.sk_security_id,
# MAGIC     dc.sk_company_id,
# MAGIC     sdm.dm_date AS market_date,
# MAGIC     sdm.dm_s_symb AS symbol,
# MAGIC     sdm.dm_close AS close_price,
# MAGIC     sdm.dm_high AS high_price,
# MAGIC     sdm.dm_low AS low_price,
# MAGIC     sdm.dm_vol AS volume,
# MAGIC     sdm.batch_id,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_daily_market sdm
# MAGIC INNER JOIN gold_dim_date dd ON sdm.dm_date = dd.date_value
# MAGIC INNER JOIN gold_dim_security ds ON sdm.dm_s_symb = ds.symbol
# MAGIC LEFT JOIN gold_dim_company dc ON ds.company_id = dc.company_id
# MAGIC WHERE sdm.batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_fact_cash_balances: Aggregate from silver_cash_transaction
# MAGIC CREATE OR REPLACE TABLE gold_fact_cash_balances AS
# MAGIC SELECT 
# MAGIC     dd.sk_date_id,
# MAGIC     da.sk_account_id,
# MAGIC     dc.sk_customer_id,
# MAGIC     sct.ct_ca_id AS account_id,
# MAGIC     SUM(sct.ct_amt) AS cash_balance,
# MAGIC     COUNT(*) AS transaction_count,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_cash_transaction sct
# MAGIC INNER JOIN gold_dim_date dd ON DATE(sct.ct_dts) = dd.date_value
# MAGIC INNER JOIN gold_dim_account da ON sct.ct_ca_id = da.account_id
# MAGIC INNER JOIN gold_dim_customer dc ON da.customer_id = dc.customer_id
# MAGIC WHERE sct.batch_id = ${var.batch_id}
# MAGIC   AND sct.is_current = true
# MAGIC GROUP BY dd.sk_date_id, da.sk_account_id, dc.sk_customer_id, sct.ct_ca_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_fact_holdings: From silver_holding_history
# MAGIC CREATE OR REPLACE TABLE gold_fact_holdings AS
# MAGIC SELECT 
# MAGIC     dd.sk_date_id,
# MAGIC     da.sk_account_id,
# MAGIC     ds.sk_security_id,
# MAGIC     st.account_id,
# MAGIC     st.symbol,
# MAGIC     shh.hh_after_qty AS quantity,  -- Use final quantity
# MAGIC     st.trade_price AS purchase_price,
# MAGIC     DATE(st.trade_dts) AS purchase_date,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_holding_history shh
# MAGIC INNER JOIN silver_trades st ON shh.hh_t_id = st.trade_id
# MAGIC INNER JOIN gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
# MAGIC INNER JOIN gold_dim_account da ON st.account_id = da.account_id
# MAGIC INNER JOIN gold_dim_security ds ON st.symbol = ds.symbol
# MAGIC WHERE shh.batch_id = ${var.batch_id}
# MAGIC   AND shh.is_current = true
# MAGIC   AND st.is_current = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_fact_watches: From silver_watch_history
# MAGIC CREATE OR REPLACE TABLE gold_fact_watches AS
# MAGIC SELECT 
# MAGIC     dc.sk_customer_id,
# MAGIC     ds.sk_security_id,
# MAGIC     swh.w_c_id AS customer_id,
# MAGIC     swh.w_s_symb AS symbol,
# MAGIC     swh.w_dts AS watch_date,
# MAGIC     swh.w_action AS watch_action,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_watch_history swh
# MAGIC INNER JOIN gold_dim_customer dc ON swh.w_c_id = dc.customer_id
# MAGIC INNER JOIN gold_dim_security ds ON swh.w_s_symb = ds.symbol
# MAGIC WHERE swh.batch_id = ${var.batch_id}
# MAGIC   AND swh.is_current = true;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Other Gold Tables
# MAGIC -- ============================================================================
# MAGIC -- gold_financials: From silver_financials
# MAGIC CREATE OR REPLACE TABLE gold_financials AS
# MAGIC SELECT 
# MAGIC     co_name_or_cik,
# MAGIC     year,
# MAGIC     quarter,
# MAGIC     qtr_start_date,
# MAGIC     posting_date,
# MAGIC     revenue,
# MAGIC     earnings,
# MAGIC     eps,
# MAGIC     diluted_eps,
# MAGIC     margin,
# MAGIC     inventory,
# MAGIC     assets,
# MAGIC     liabilities,
# MAGIC     sh_out,
# MAGIC     diluted_sh_out,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_financials
# MAGIC WHERE batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_prospect: From silver_prospect
# MAGIC CREATE OR REPLACE TABLE gold_prospect AS
# MAGIC SELECT 
# MAGIC     agency_id,
# MAGIC     last_name,
# MAGIC     first_name,
# MAGIC     middle_initial,
# MAGIC     gender,
# MAGIC     address_line1,
# MAGIC     address_line2,
# MAGIC     postal_code,
# MAGIC     city,
# MAGIC     state,
# MAGIC     country,
# MAGIC     phone,
# MAGIC     income,
# MAGIC     number_cars,
# MAGIC     number_children,
# MAGIC     marital_status,
# MAGIC     age,
# MAGIC     credit_rating,
# MAGIC     own_or_rent_flag,
# MAGIC     employer,
# MAGIC     is_customer,
# MAGIC     net_worth,
# MAGIC     marketing_nameplate,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_prospect
# MAGIC WHERE batch_id = ${var.batch_id};

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Load completed' AS status;