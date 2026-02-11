# Databricks notebook source
# MAGIC %md
# MAGIC # Load Gold Incremental Data
# MAGIC
# MAGIC Loads incremental Silver data into Gold tables

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

# MAGIC %md
# MAGIC ## Create Gold Tables
# MAGIC
# MAGIC Create all gold tables before loading data.

# COMMAND ----------

# Get the current notebook path to determine the base path for table creation notebooks
import os
current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
base_path = os.path.dirname(current_notebook_path)
tables_path = f"{base_path}/tables"

# List of all gold tables to create (in order)
gold_tables = [
    "gold_dim_date",
    "gold_dim_time",
    "gold_dim_trade_type",
    "gold_dim_status_type",
    "gold_dim_industry",
    "gold_dim_company",
    "gold_dim_security",
    "gold_dim_customer",
    "gold_dim_account",
    "gold_dim_broker",
    "gold_dim_trade",
    "gold_dim_messages",
    "gold_fact_trade",
    "gold_fact_market_history",
    "gold_fact_cash_balances",
    "gold_fact_holdings",
    "gold_fact_watches",
    "gold_financials",
    "gold_prospect"
]

# Create all gold tables
for table_name in gold_tables:
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
# COMMAND ----------

# Set catalog and create/use schema
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name}")
spark.sql(f"USE {catalog}.{schema_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- gold_dim_customer: MERGE upsert (SCD Type 1 - latest only)

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

# COMMAND ----------

spark.sql(f"""
-- gold_dim_account: MERGE upsert
MERGE INTO gold_dim_account AS target
USING (
    SELECT 
        monotonically_increasing_id() AS sk_account_id,
        account_id,
        broker_id,
        customer_id,
        account_name,
        tax_status,
        status_id
    FROM silver_accounts
    WHERE is_current = true
      AND batch_id = {batch_id}
      AND account_id != -1
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY effective_date DESC) = 1
) AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET
    target.broker_id = source.broker_id,
    target.customer_id = source.customer_id,
    target.account_name = source.account_name,
    target.tax_status = source.tax_status,
    target.status_id = source.status_id,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- gold_dim_security: MERGE upsert (new securities may appear)
MERGE INTO gold_dim_security AS target
USING (
    SELECT 
        ss.symbol AS sk_security_id,
        ss.symbol AS security_id,
        ss.symbol,
        ss.issue_type,
        ss.status,
        ss.name,
        ss.ex_id AS exchange_id,
        ss.sh_out AS shares_outstanding,
        ss.first_trade_date,
        ss.first_trade_exchg AS first_trade_exchange,
        ss.dividend,
        ss.co_name_or_cik AS company_id
    FROM silver_securities ss
    WHERE ss.batch_id = {batch_id}
) AS source
ON target.symbol = source.symbol
WHEN MATCHED THEN UPDATE SET
    target.issue_type = source.issue_type,
    target.status = source.status,
    target.name = source.name,
    target.exchange_id = source.exchange_id,
    target.shares_outstanding = source.shares_outstanding,
    target.first_trade_date = source.first_trade_date,
    target.first_trade_exchange = source.first_trade_exchange,
    target.dividend = source.dividend,
    target.company_id = source.company_id,
    target.is_current = true,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    sk_security_id, security_id, symbol, issue_type, status, name,
    exchange_id, shares_outstanding, first_trade_date, first_trade_exchange,
    dividend, company_id, is_current, etl_timestamp
) VALUES (
    source.sk_security_id, source.security_id, source.symbol, source.issue_type,
    source.status, source.name, source.exchange_id, source.shares_outstanding,
    source.first_trade_date, source.first_trade_exchange, source.dividend,
    source.company_id, true, current_timestamp()
);
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- gold_dim_company: MERGE upsert
MERGE INTO gold_dim_company AS target
USING (
    SELECT 
        sc.sk_company_id,
        sc.company_id,
        sc.company_name,
        sc.industry_id,
        si.in_sc_id AS sector,
        sc.status,
        sc.address_line1,
        sc.address_line2,
        sc.postal_code,
        sc.city,
        sc.state_province AS state_prov,
        sc.country,
        sc.description,
        sc.founding_date,
        sc.ceo_name
    FROM silver_companies sc
    LEFT JOIN silver_industry si ON sc.industry_id = si.in_id
    WHERE sc.batch_id = {batch_id}
) AS source
ON target.company_id = source.company_id
WHEN MATCHED THEN UPDATE SET
    target.company_name = source.company_name,
    target.industry_id = source.industry_id,
    target.sector = source.sector,
    target.status = source.status,
    target.address_line1 = source.address_line1,
    target.address_line2 = source.address_line2,
    target.postal_code = source.postal_code,
    target.city = source.city,
    target.state_prov = source.state_prov,
    target.country = source.country,
    target.description = source.description,
    target.founding_date = source.founding_date,
    target.ceo_name = source.ceo_name,
    target.is_current = true,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    sk_company_id, company_id, company_name, industry_id, sector, status,
    address_line1, address_line2, postal_code, city, state_prov, country,
    description, founding_date, ceo_name, is_current, etl_timestamp
) VALUES (
    source.sk_company_id, source.company_id, source.company_name, source.industry_id,
    source.sector, source.status, source.address_line1, source.address_line2,
    source.postal_code, source.city, source.state_prov, source.country,
    source.description, source.founding_date, source.ceo_name, true, current_timestamp()
);
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- gold_financials: MERGE upsert (SCD Type 1 - latest only)
MERGE INTO gold_financials AS target
USING (
    SELECT 
        co_name_or_cik,
        year,
        quarter,
        qtr_start_date,
        posting_date,
        revenue,
        earnings,
        eps,
        diluted_eps,
        margin,
        inventory,
        assets,
        liabilities,
        sh_out,
        diluted_sh_out
    FROM silver_financials
    WHERE batch_id = {batch_id}
) AS source
ON target.co_name_or_cik = source.co_name_or_cik
   AND target.year = source.year
   AND target.quarter = source.quarter
WHEN MATCHED THEN UPDATE SET
    target.qtr_start_date = source.qtr_start_date,
    target.posting_date = source.posting_date,
    target.revenue = source.revenue,
    target.earnings = source.earnings,
    target.eps = source.eps,
    target.diluted_eps = source.diluted_eps,
    target.margin = source.margin,
    target.inventory = source.inventory,
    target.assets = source.assets,
    target.liabilities = source.liabilities,
    target.sh_out = source.sh_out,
    target.diluted_sh_out = source.diluted_sh_out,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
""")

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

# COMMAND ----------

spark.sql(f"""
-- Log late-arriving trades to DI_Messages
INSERT INTO gold_dim_messages
SELECT 
    current_timestamp() AS message_timestamp,
    {batch_id} AS batch_id,
    'FactTrade' AS originating_table,
    CONCAT('Late-arriving trade: TradeID=', st.trade_id, ' AccountID=', st.account_id) AS message_text,
    'Alert' AS message_type,
    'Gold_FactTrade_Load' AS component_name,
    'Warning' AS severity
FROM silver_trades st
LEFT JOIN gold_dim_account da ON st.account_id = da.account_id
WHERE st.batch_id = {batch_id}
  AND st.is_current = true
  AND da.account_id IS NULL;  -- Account not found
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- gold_fact_market_history: Append new market data
INSERT INTO gold_fact_market_history
SELECT 
    dd.sk_date_id,
    ds.sk_security_id,
    dc.sk_company_id,
    sdm.dm_date AS market_date,
    sdm.dm_s_symb AS symbol,
    sdm.dm_close AS close_price,
    sdm.dm_high AS high_price,
    sdm.dm_low AS low_price,
    sdm.dm_vol AS volume,
    sdm.batch_id,
    current_timestamp() AS etl_timestamp
FROM silver_daily_market sdm
INNER JOIN gold_dim_date dd ON sdm.dm_date = dd.date_value
INNER JOIN gold_dim_security ds ON sdm.dm_s_symb = ds.symbol
LEFT JOIN gold_dim_company dc ON ds.company_id = dc.company_id
WHERE sdm.batch_id = {batch_id};
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- gold_fact_cash_balances: MERGE (update existing, insert new)
MERGE INTO gold_fact_cash_balances AS target
USING (
    SELECT 
        dd.sk_date_id,
        da.sk_account_id,
        dc.sk_customer_id,
        sct.ct_ca_id AS account_id,
        SUM(sct.ct_amt) AS cash_balance,
        COUNT(*) AS transaction_count
    FROM silver_cash_transaction sct
    INNER JOIN gold_dim_date dd ON DATE(sct.ct_dts) = dd.date_value
    INNER JOIN gold_dim_account da ON sct.ct_ca_id = da.account_id
    INNER JOIN gold_dim_customer dc ON da.customer_id = dc.customer_id
    WHERE sct.batch_id = {batch_id}
      AND sct.is_current = true
    GROUP BY dd.sk_date_id, da.sk_account_id, dc.sk_customer_id, sct.ct_ca_id
) AS source
ON target.sk_date_id = source.sk_date_id
   AND target.sk_account_id = source.sk_account_id
WHEN MATCHED THEN UPDATE SET
    target.cash_balance = source.cash_balance,
    target.transaction_count = source.transaction_count,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- gold_fact_holdings: MERGE (update existing holdings)
MERGE INTO gold_fact_holdings AS target
USING (
    SELECT 
        dd.sk_date_id,
        da.sk_account_id,
        ds.sk_security_id,
        st.account_id,
        st.symbol,
        shh.hh_after_qty AS quantity,
        st.trade_price AS purchase_price,
        DATE(st.trade_dts) AS purchase_date
    FROM silver_holding_history shh
    INNER JOIN silver_trades st ON shh.hh_t_id = st.trade_id
    INNER JOIN gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
    INNER JOIN gold_dim_account da ON st.account_id = da.account_id
    INNER JOIN gold_dim_security ds ON st.symbol = ds.symbol
    WHERE shh.batch_id = {batch_id}
      AND shh.is_current = true
      AND st.is_current = true
) AS source
ON target.sk_date_id = source.sk_date_id
   AND target.sk_account_id = source.sk_account_id
   AND target.sk_security_id = source.sk_security_id
WHEN MATCHED THEN UPDATE SET
    target.quantity = source.quantity,
    target.purchase_price = source.purchase_price,
    target.purchase_date = source.purchase_date,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- gold_fact_watches: Append new watches
INSERT INTO gold_fact_watches
SELECT 
    dc.sk_customer_id,
    ds.sk_security_id,
    swh.w_c_id AS customer_id,
    swh.w_s_symb AS symbol,
    swh.w_dts AS watch_date,
    swh.w_action AS watch_action,
    current_timestamp() AS etl_timestamp
FROM silver_watch_history swh
INNER JOIN gold_dim_customer dc ON swh.w_c_id = dc.customer_id
INNER JOIN gold_dim_security ds ON swh.w_s_symb = ds.symbol
WHERE swh.batch_id = {batch_id}
  AND swh.is_current = true
  AND swh.record_type IN ('I', 'U');
""")

# COMMAND ----------

# COMMAND ----------

spark.sql(f"""
-- gold_prospect: MERGE upsert
MERGE INTO gold_prospect AS target
USING (
    SELECT 
        agency_id,
        last_name,
        first_name,
        middle_initial,
        gender,
        address_line1,
        address_line2,
        postal_code,
        city,
        state,
        country,
        phone,
        income,
        number_cars,
        number_children,
        marital_status,
        age,
        credit_rating,
        own_or_rent_flag,
        employer,
        is_customer,
        net_worth,
        marketing_nameplate
    FROM silver_prospect
    WHERE batch_id = {batch_id}
) AS source
ON target.agency_id = source.agency_id
WHEN MATCHED THEN UPDATE SET
    target.last_name = source.last_name,
    target.first_name = source.first_name,
    target.middle_initial = source.middle_initial,
    target.gender = source.gender,
    target.address_line1 = source.address_line1,
    target.address_line2 = source.address_line2,
    target.postal_code = source.postal_code,
    target.city = source.city,
    target.state = source.state,
    target.country = source.country,
    target.phone = source.phone,
    target.income = source.income,
    target.number_cars = source.number_cars,
    target.number_children = source.number_children,
    target.marital_status = source.marital_status,
    target.age = source.age,
    target.credit_rating = source.credit_rating,
    target.own_or_rent_flag = source.own_or_rent_flag,
    target.employer = source.employer,
    target.is_customer = source.is_customer,
    target.net_worth = source.net_worth,
    target.marketing_nameplate = source.marketing_nameplate,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Load completed' AS status;