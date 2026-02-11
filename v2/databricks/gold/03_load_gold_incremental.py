# Databricks notebook source
# MAGIC %md
# MAGIC # Load Gold Incremental Data
# MAGIC
# MAGIC Loads incremental Silver data into Gold tables

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

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- TPC-DI v2: Gold Layer - Incremental Load (Batch 2+)
# MAGIC -- ============================================================================
# MAGIC -- Loads Silver data into Gold star schema tables using MERGE
# MAGIC -- Batch 2+: MERGE for dimensions (SCD Type 1), APPEND for facts
# MAGIC -- ============================================================================
# MAGIC -- Set variables
# MAGIC -- SET var.batch_id = 2;  -- Change for Batch 3, 4, etc.
# MAGIC -- ============================================================================
# MAGIC -- Dimension Tables (Batch 2+: MERGE upsert)
# MAGIC -- ============================================================================
# MAGIC -- gold_dim_customer: MERGE upsert (SCD Type 1 - latest only)
# MAGIC USE CATALOG ${var.catalog};

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA ${var.schema};

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold_dim_customer AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         sk_customer_id,
# MAGIC         customer_id,
# MAGIC         tax_id,
# MAGIC         status,
# MAGIC         last_name,
# MAGIC         first_name,
# MAGIC         middle_name,
# MAGIC         gender,
# MAGIC         tier,
# MAGIC         dob,
# MAGIC         address_line1,
# MAGIC         address_line2,
# MAGIC         postal_code,
# MAGIC         city,
# MAGIC         state_prov,
# MAGIC         country,
# MAGIC         email1,
# MAGIC         email2,
# MAGIC         local_tax_id,
# MAGIC         national_tax_id
# MAGIC     FROM silver_customers
# MAGIC     WHERE is_current = true
# MAGIC       AND batch_id = ${var.batch_id}
# MAGIC       AND customer_id != -1
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY effective_date DESC) = 1  -- Deduplicate
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.sk_customer_id = source.sk_customer_id,
# MAGIC     target.tax_id = source.tax_id,
# MAGIC     target.status = source.status,
# MAGIC     target.last_name = source.last_name,
# MAGIC     target.first_name = source.first_name,
# MAGIC     target.middle_name = source.middle_name,
# MAGIC     target.gender = source.gender,
# MAGIC     target.tier = source.tier,
# MAGIC     target.dob = source.dob,
# MAGIC     target.address_line1 = source.address_line1,
# MAGIC     target.address_line2 = source.address_line2,
# MAGIC     target.postal_code = source.postal_code,
# MAGIC     target.city = source.city,
# MAGIC     target.state_prov = source.state_prov,
# MAGIC     target.country = source.country,
# MAGIC     target.email1 = source.email1,
# MAGIC     target.email2 = source.email2,
# MAGIC     target.local_tax_id = source.local_tax_id,
# MAGIC     target.national_tax_id = source.national_tax_id,
# MAGIC     target.etl_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_account: MERGE upsert
# MAGIC MERGE INTO gold_dim_account AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         monotonically_increasing_id() AS sk_account_id,
# MAGIC         account_id,
# MAGIC         broker_id,
# MAGIC         customer_id,
# MAGIC         account_name,
# MAGIC         tax_status,
# MAGIC         status_id
# MAGIC     FROM silver_accounts
# MAGIC     WHERE is_current = true
# MAGIC       AND batch_id = ${var.batch_id}
# MAGIC       AND account_id != -1
# MAGIC     QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY effective_date DESC) = 1
# MAGIC ) AS source
# MAGIC ON target.account_id = source.account_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.broker_id = source.broker_id,
# MAGIC     target.customer_id = source.customer_id,
# MAGIC     target.account_name = source.account_name,
# MAGIC     target.tax_status = source.tax_status,
# MAGIC     target.status_id = source.status_id,
# MAGIC     target.etl_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_security: MERGE upsert (new securities may appear)
# MAGIC MERGE INTO gold_dim_security AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         ss.symbol AS sk_security_id,
# MAGIC         ss.symbol AS security_id,
# MAGIC         ss.symbol,
# MAGIC         ss.issue_type,
# MAGIC         ss.status,
# MAGIC         ss.name,
# MAGIC         ss.ex_id AS exchange_id,
# MAGIC         ss.sh_out AS shares_outstanding,
# MAGIC         ss.first_trade_date,
# MAGIC         ss.first_trade_exchg AS first_trade_exchange,
# MAGIC         ss.dividend,
# MAGIC         ss.co_name_or_cik AS company_id
# MAGIC     FROM silver_securities ss
# MAGIC     WHERE ss.batch_id = ${var.batch_id}
# MAGIC ) AS source
# MAGIC ON target.symbol = source.symbol
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.issue_type = source.issue_type,
# MAGIC     target.status = source.status,
# MAGIC     target.name = source.name,
# MAGIC     target.exchange_id = source.exchange_id,
# MAGIC     target.shares_outstanding = source.shares_outstanding,
# MAGIC     target.first_trade_date = source.first_trade_date,
# MAGIC     target.first_trade_exchange = source.first_trade_exchange,
# MAGIC     target.dividend = source.dividend,
# MAGIC     target.company_id = source.company_id,
# MAGIC     target.is_current = true,
# MAGIC     target.etl_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     sk_security_id, security_id, symbol, issue_type, status, name,
# MAGIC     exchange_id, shares_outstanding, first_trade_date, first_trade_exchange,
# MAGIC     dividend, company_id, is_current, etl_timestamp
# MAGIC ) VALUES (
# MAGIC     source.sk_security_id, source.security_id, source.symbol, source.issue_type,
# MAGIC     source.status, source.name, source.exchange_id, source.shares_outstanding,
# MAGIC     source.first_trade_date, source.first_trade_exchange, source.dividend,
# MAGIC     source.company_id, true, current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_company: MERGE upsert
# MAGIC MERGE INTO gold_dim_company AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         sc.sk_company_id,
# MAGIC         sc.company_id,
# MAGIC         sc.company_name,
# MAGIC         sc.industry_id,
# MAGIC         si.in_sc_id AS sector,
# MAGIC         sc.status,
# MAGIC         sc.address_line1,
# MAGIC         sc.address_line2,
# MAGIC         sc.postal_code,
# MAGIC         sc.city,
# MAGIC         sc.state_province AS state_prov,
# MAGIC         sc.country,
# MAGIC         sc.description,
# MAGIC         sc.founding_date,
# MAGIC         sc.ceo_name
# MAGIC     FROM silver_companies sc
# MAGIC     LEFT JOIN silver_industry si ON sc.industry_id = si.in_id
# MAGIC     WHERE sc.batch_id = ${var.batch_id}
# MAGIC ) AS source
# MAGIC ON target.company_id = source.company_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.company_name = source.company_name,
# MAGIC     target.industry_id = source.industry_id,
# MAGIC     target.sector = source.sector,
# MAGIC     target.status = source.status,
# MAGIC     target.address_line1 = source.address_line1,
# MAGIC     target.address_line2 = source.address_line2,
# MAGIC     target.postal_code = source.postal_code,
# MAGIC     target.city = source.city,
# MAGIC     target.state_prov = source.state_prov,
# MAGIC     target.country = source.country,
# MAGIC     target.description = source.description,
# MAGIC     target.founding_date = source.founding_date,
# MAGIC     target.ceo_name = source.ceo_name,
# MAGIC     target.is_current = true,
# MAGIC     target.etl_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     sk_company_id, company_id, company_name, industry_id, sector, status,
# MAGIC     address_line1, address_line2, postal_code, city, state_prov, country,
# MAGIC     description, founding_date, ceo_name, is_current, etl_timestamp
# MAGIC ) VALUES (
# MAGIC     source.sk_company_id, source.company_id, source.company_name, source.industry_id,
# MAGIC     source.sector, source.status, source.address_line1, source.address_line2,
# MAGIC     source.postal_code, source.city, source.state_prov, source.country,
# MAGIC     source.description, source.founding_date, source.ceo_name, true, current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_financials: MERGE upsert (SCD Type 1 - latest only)
# MAGIC MERGE INTO gold_financials AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         co_name_or_cik,
# MAGIC         year,
# MAGIC         quarter,
# MAGIC         qtr_start_date,
# MAGIC         posting_date,
# MAGIC         revenue,
# MAGIC         earnings,
# MAGIC         eps,
# MAGIC         diluted_eps,
# MAGIC         margin,
# MAGIC         inventory,
# MAGIC         assets,
# MAGIC         liabilities,
# MAGIC         sh_out,
# MAGIC         diluted_sh_out
# MAGIC     FROM silver_financials
# MAGIC     WHERE batch_id = ${var.batch_id}
# MAGIC ) AS source
# MAGIC ON target.co_name_or_cik = source.co_name_or_cik
# MAGIC    AND target.year = source.year
# MAGIC    AND target.quarter = source.quarter
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.qtr_start_date = source.qtr_start_date,
# MAGIC     target.posting_date = source.posting_date,
# MAGIC     target.revenue = source.revenue,
# MAGIC     target.earnings = source.earnings,
# MAGIC     target.eps = source.eps,
# MAGIC     target.diluted_eps = source.diluted_eps,
# MAGIC     target.margin = source.margin,
# MAGIC     target.inventory = source.inventory,
# MAGIC     target.assets = source.assets,
# MAGIC     target.liabilities = source.liabilities,
# MAGIC     target.sh_out = source.sh_out,
# MAGIC     target.diluted_sh_out = source.diluted_sh_out,
# MAGIC     target.etl_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================================
# MAGIC -- Fact Tables (Batch 2+: APPEND - facts are typically immutable)
# MAGIC -- ============================================================================
# MAGIC -- gold_fact_trade: Append new trades
# MAGIC INSERT INTO gold_fact_trade
# MAGIC SELECT 
# MAGIC     st.trade_id AS sk_trade_id,
# MAGIC     dd.sk_date_id,
# MAGIC     dt.sk_time_id,
# MAGIC     COALESCE(dc.sk_customer_id, -1) AS sk_customer_id,  -- Placeholder if missing
# MAGIC     COALESCE(da.sk_account_id, -1) AS sk_account_id,
# MAGIC     COALESCE(ds.sk_security_id, 'UNKNOWN') AS sk_security_id,
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
# MAGIC     CASE WHEN dc.sk_customer_id IS NULL OR da.sk_account_id IS NULL THEN true ELSE false END AS late_arriving_flag,
# MAGIC     current_timestamp() AS etl_timestamp
# MAGIC FROM silver_trades st
# MAGIC INNER JOIN gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
# MAGIC LEFT JOIN gold_dim_time dt ON HOUR(st.trade_dts) = dt.hour_id
# MAGIC LEFT JOIN gold_dim_account da ON st.account_id = da.account_id
# MAGIC LEFT JOIN gold_dim_customer dc ON da.customer_id = dc.customer_id
# MAGIC INNER JOIN gold_dim_security ds ON st.symbol = ds.symbol
# MAGIC INNER JOIN gold_dim_trade_type dtt ON st.trade_type_id = dtt.trade_type_id
# MAGIC WHERE st.batch_id = ${var.batch_id}
# MAGIC   AND st.is_current = true
# MAGIC   AND st.record_type IN ('I', 'U');  -- Only new/updated trades

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Log late-arriving trades to DI_Messages
# MAGIC INSERT INTO gold_dim_messages
# MAGIC SELECT 
# MAGIC     current_timestamp() AS message_timestamp,
# MAGIC     ${var.batch_id} AS batch_id,
# MAGIC     'FactTrade' AS originating_table,
# MAGIC     CONCAT('Late-arriving trade: TradeID=', st.trade_id, ' AccountID=', st.account_id) AS message_text,
# MAGIC     'Alert' AS message_type,
# MAGIC     'Gold_FactTrade_Load' AS component_name,
# MAGIC     'Warning' AS severity
# MAGIC FROM silver_trades st
# MAGIC LEFT JOIN gold_dim_account da ON st.account_id = da.account_id
# MAGIC WHERE st.batch_id = ${var.batch_id}
# MAGIC   AND st.is_current = true
# MAGIC   AND da.account_id IS NULL;  -- Account not found

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_fact_market_history: Append new market data
# MAGIC INSERT INTO gold_fact_market_history
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
# MAGIC -- gold_fact_cash_balances: MERGE (update existing, insert new)
# MAGIC MERGE INTO gold_fact_cash_balances AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         dd.sk_date_id,
# MAGIC         da.sk_account_id,
# MAGIC         dc.sk_customer_id,
# MAGIC         sct.ct_ca_id AS account_id,
# MAGIC         SUM(sct.ct_amt) AS cash_balance,
# MAGIC         COUNT(*) AS transaction_count
# MAGIC     FROM silver_cash_transaction sct
# MAGIC     INNER JOIN gold_dim_date dd ON DATE(sct.ct_dts) = dd.date_value
# MAGIC     INNER JOIN gold_dim_account da ON sct.ct_ca_id = da.account_id
# MAGIC     INNER JOIN gold_dim_customer dc ON da.customer_id = dc.customer_id
# MAGIC     WHERE sct.batch_id = ${var.batch_id}
# MAGIC       AND sct.is_current = true
# MAGIC     GROUP BY dd.sk_date_id, da.sk_account_id, dc.sk_customer_id, sct.ct_ca_id
# MAGIC ) AS source
# MAGIC ON target.sk_date_id = source.sk_date_id
# MAGIC    AND target.sk_account_id = source.sk_account_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.cash_balance = source.cash_balance,
# MAGIC     target.transaction_count = source.transaction_count,
# MAGIC     target.etl_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_fact_holdings: MERGE (update existing holdings)
# MAGIC MERGE INTO gold_fact_holdings AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         dd.sk_date_id,
# MAGIC         da.sk_account_id,
# MAGIC         ds.sk_security_id,
# MAGIC         st.account_id,
# MAGIC         st.symbol,
# MAGIC         shh.hh_after_qty AS quantity,
# MAGIC         st.trade_price AS purchase_price,
# MAGIC         DATE(st.trade_dts) AS purchase_date
# MAGIC     FROM silver_holding_history shh
# MAGIC     INNER JOIN silver_trades st ON shh.hh_t_id = st.trade_id
# MAGIC     INNER JOIN gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
# MAGIC     INNER JOIN gold_dim_account da ON st.account_id = da.account_id
# MAGIC     INNER JOIN gold_dim_security ds ON st.symbol = ds.symbol
# MAGIC     WHERE shh.batch_id = ${var.batch_id}
# MAGIC       AND shh.is_current = true
# MAGIC       AND st.is_current = true
# MAGIC ) AS source
# MAGIC ON target.sk_date_id = source.sk_date_id
# MAGIC    AND target.sk_account_id = source.sk_account_id
# MAGIC    AND target.sk_security_id = source.sk_security_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.quantity = source.quantity,
# MAGIC     target.purchase_price = source.purchase_price,
# MAGIC     target.purchase_date = source.purchase_date,
# MAGIC     target.etl_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_fact_watches: Append new watches
# MAGIC INSERT INTO gold_fact_watches
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
# MAGIC   AND swh.is_current = true
# MAGIC   AND swh.record_type IN ('I', 'U');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_prospect: MERGE upsert
# MAGIC MERGE INTO gold_prospect AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         agency_id,
# MAGIC         last_name,
# MAGIC         first_name,
# MAGIC         middle_initial,
# MAGIC         gender,
# MAGIC         address_line1,
# MAGIC         address_line2,
# MAGIC         postal_code,
# MAGIC         city,
# MAGIC         state,
# MAGIC         country,
# MAGIC         phone,
# MAGIC         income,
# MAGIC         number_cars,
# MAGIC         number_children,
# MAGIC         marital_status,
# MAGIC         age,
# MAGIC         credit_rating,
# MAGIC         own_or_rent_flag,
# MAGIC         employer,
# MAGIC         number_credit_cards,
# MAGIC         net_worth
# MAGIC     FROM silver_prospect
# MAGIC     WHERE batch_id = ${var.batch_id}
# MAGIC ) AS source
# MAGIC ON target.agency_id = source.agency_id
# MAGIC WHEN MATCHED THEN UPDATE SET
# MAGIC     target.last_name = source.last_name,
# MAGIC     target.first_name = source.first_name,
# MAGIC     target.middle_initial = source.middle_initial,
# MAGIC     target.gender = source.gender,
# MAGIC     target.address_line1 = source.address_line1,
# MAGIC     target.address_line2 = source.address_line2,
# MAGIC     target.postal_code = source.postal_code,
# MAGIC     target.city = source.city,
# MAGIC     target.state = source.state,
# MAGIC     target.country = source.country,
# MAGIC     target.phone = source.phone,
# MAGIC     target.income = source.income,
# MAGIC     target.number_cars = source.number_cars,
# MAGIC     target.number_children = source.number_children,
# MAGIC     target.marital_status = source.marital_status,
# MAGIC     target.age = source.age,
# MAGIC     target.credit_rating = source.credit_rating,
# MAGIC     target.own_or_rent_flag = source.own_or_rent_flag,
# MAGIC     target.employer = source.employer,
# MAGIC     target.number_credit_cards = source.number_credit_cards,
# MAGIC     target.net_worth = source.net_worth,
# MAGIC     target.etl_timestamp = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Load completed' AS status;