-- ============================================================================
-- TPC-DI v2: Gold Layer - Batch 1 Load (Historical)
-- ============================================================================
-- Loads Silver data into Gold star schema tables
-- Batch 1: Bulk INSERT (no MERGE needed)
-- ============================================================================

-- Set variables
-- SET var.batch_id = 1;

-- ============================================================================
-- Dimension Tables (Batch 1: INSERT)
-- ============================================================================

-- gold_dim_customer: Current versions only from silver_customers
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};


INSERT INTO gold_dim_customer
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
    current_timestamp() AS etl_timestamp
FROM silver_customers
WHERE is_current = true
  AND batch_id = ${var.batch_id}
  AND customer_id != -1;  -- Exclude placeholder

-- gold_dim_account: Current versions only from silver_accounts
INSERT INTO gold_dim_account
SELECT 
    monotonically_increasing_id() AS sk_account_id,
    account_id,
    broker_id,
    customer_id,
    account_name,
    tax_status,
    status_id,
    current_timestamp() AS etl_timestamp
FROM silver_accounts
WHERE is_current = true
  AND batch_id = ${var.batch_id}
  AND account_id != -1;  -- Exclude placeholder

-- gold_dim_date: From silver_date
INSERT INTO gold_dim_date
SELECT 
    sk_date_id AS sk_date_id,
    sk_date_id AS date_id,
    date_value,
    date_desc,
    calendar_year_id,
    calendar_year_desc,
    calendar_qtr_id,
    calendar_qtr_desc,
    calendar_month_id,
    calendar_month_desc,
    calendar_week_id,
    calendar_week_desc,
    day_of_week_num,
    day_of_week_desc,
    fiscal_year_id,
    fiscal_year_desc,
    fiscal_qtr_id,
    fiscal_qtr_desc,
    holiday_flag,
    current_timestamp() AS etl_timestamp
FROM silver_date
WHERE batch_id = ${var.batch_id};

-- gold_dim_time: From silver_time
INSERT INTO gold_dim_time
SELECT 
    sk_time_id AS sk_time_id,
    sk_time_id AS time_id,
    time_value,
    hour_id,
    hour_desc,
    minute_id,
    minute_desc,
    second_id,
    second_desc,
    market_hours_flag,
    office_hours_flag,
    current_timestamp() AS etl_timestamp
FROM silver_time
WHERE batch_id = ${var.batch_id};

-- gold_dim_trade_type: From silver_trade_type
INSERT INTO gold_dim_trade_type
SELECT 
    tt_id AS sk_trade_type_id,
    tt_id AS trade_type_id,
    tt_id AS trade_type_code,
    tt_name AS trade_type_name,
    tt_is_sell AS is_sell,
    tt_is_mrkt AS is_market,
    current_timestamp() AS etl_timestamp
FROM silver_trade_type
WHERE batch_id = ${var.batch_id};

-- gold_dim_status_type: From silver_status_type
INSERT INTO gold_dim_status_type
SELECT 
    st_id AS sk_status_type_id,
    st_id AS status_type_id,
    st_id AS status_type_code,
    st_name AS status_type_name,
    current_timestamp() AS etl_timestamp
FROM silver_status_type
WHERE batch_id = ${var.batch_id};

-- gold_dim_industry: From silver_industry
INSERT INTO gold_dim_industry
SELECT 
    in_id AS sk_industry_id,
    in_id AS industry_id,
    in_name AS industry_name,
    in_sc_id AS sector_id,
    NULL AS sector_name,  -- Lookup or derive if needed
    current_timestamp() AS etl_timestamp
FROM silver_industry
WHERE batch_id = ${var.batch_id};

-- gold_dim_company: From silver_companies (current only)
INSERT INTO gold_dim_company
SELECT 
    sc.sk_company_id,
    sc.company_id,
    sc.company_name,
    sc.industry_id,
    si.in_sc_id AS sector,  -- Join to industry for sector
    sc.status,
    sc.address_line1,
    sc.address_line2,
    sc.postal_code,
    sc.city,
    sc.state_province AS state_prov,
    sc.country,
    sc.description,
    sc.founding_date,
    sc.ceo_name,
    TRUE AS is_current,
    current_timestamp() AS etl_timestamp
FROM silver_companies sc
LEFT JOIN silver_industry si ON sc.industry_id = si.in_id
WHERE sc.batch_id = ${var.batch_id};

-- gold_dim_security: From silver_securities (current only)
INSERT INTO gold_dim_security
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
    ss.co_name_or_cik AS company_id,  -- Reference to DimCompany
    TRUE AS is_current,
    current_timestamp() AS etl_timestamp
FROM silver_securities ss
WHERE ss.batch_id = ${var.batch_id};

-- gold_dim_broker: From silver_hr (extract brokers)
-- Note: This assumes HR.csv has been parsed and brokers identified
-- Adjust based on your HR parsing logic
INSERT INTO gold_dim_broker
SELECT 
    monotonically_increasing_id() AS sk_broker_id,
    CAST(employee_id AS BIGINT) AS broker_id,
    CONCAT(first_name, ' ', last_name) AS broker_name,
    branch AS branch,
    office AS office,
    phone AS phone,
    TRUE AS is_current,
    current_timestamp() AS etl_timestamp
FROM (
    -- Parse HR.csv: EmployeeID|ManagerID|EmployeeFirstName|EmployeeLastName|...
    -- Filter where job code indicates broker
    SELECT DISTINCT
        split(raw_line, ',')[0] AS employee_id,
        split(raw_line, ',')[1] AS manager_id,
        split(raw_line, ',')[2] AS first_name,
        split(raw_line, ',')[3] AS last_name,
        split(raw_line, ',')[4] AS branch,
        split(raw_line, ',')[5] AS office,
        split(raw_line, ',')[6] AS phone,
        split(raw_line, ',')[7] AS job_code
    FROM bronze_hr
    WHERE _batch_id = ${var.batch_id}
      AND raw_line IS NOT NULL
      AND split(raw_line, ',')[7] LIKE '%BROKER%'  -- Adjust filter as needed
) AS brokers;

-- ============================================================================
-- Fact Tables (Batch 1: INSERT)
-- ============================================================================

-- gold_fact_trade: Join trades with dimensions
INSERT INTO gold_fact_trade
SELECT 
    st.trade_id AS sk_trade_id,  -- Use trade_id as surrogate key
    dd.sk_date_id,
    dt.sk_time_id,
    dc.sk_customer_id,
    da.sk_account_id,
    ds.sk_security_id,
    dtt.sk_trade_type_id,
    st.trade_id,
    st.trade_dts,
    st.trade_price,
    st.quantity AS trade_quantity,
    st.trade_price * st.quantity AS trade_amount,
    st.commission,
    st.charge,
    st.tax,
    st.status_id,
    st.is_cash,
    st.exec_name,
    st.batch_id,
    FALSE AS late_arriving_flag,  -- Batch 1 has no late arrivals
    current_timestamp() AS etl_timestamp
FROM silver_trades st
INNER JOIN gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
LEFT JOIN gold_dim_time dt ON HOUR(st.trade_dts) = dt.hour_id
INNER JOIN gold_dim_account da ON st.account_id = da.account_id
INNER JOIN gold_dim_customer dc ON da.customer_id = dc.customer_id
INNER JOIN gold_dim_security ds ON st.symbol = ds.symbol
INNER JOIN gold_dim_trade_type dtt ON st.trade_type_id = dtt.trade_type_id
WHERE st.batch_id = ${var.batch_id}
  AND st.is_current = true;

-- gold_fact_market_history: From silver_daily_market
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
WHERE sdm.batch_id = ${var.batch_id};

-- gold_fact_cash_balances: Aggregate from silver_cash_transaction
INSERT INTO gold_fact_cash_balances
SELECT 
    dd.sk_date_id,
    da.sk_account_id,
    dc.sk_customer_id,
    sct.ct_ca_id AS account_id,
    SUM(sct.ct_amt) AS cash_balance,
    COUNT(*) AS transaction_count,
    current_timestamp() AS etl_timestamp
FROM silver_cash_transaction sct
INNER JOIN gold_dim_date dd ON DATE(sct.ct_dts) = dd.date_value
INNER JOIN gold_dim_account da ON sct.ct_ca_id = da.account_id
INNER JOIN gold_dim_customer dc ON da.customer_id = dc.customer_id
WHERE sct.batch_id = ${var.batch_id}
  AND sct.is_current = true
GROUP BY dd.sk_date_id, da.sk_account_id, dc.sk_customer_id, sct.ct_ca_id;

-- gold_fact_holdings: From silver_holding_history
INSERT INTO gold_fact_holdings
SELECT 
    dd.sk_date_id,
    da.sk_account_id,
    ds.sk_security_id,
    st.account_id,
    st.symbol,
    shh.hh_after_qty AS quantity,  -- Use final quantity
    st.trade_price AS purchase_price,
    DATE(st.trade_dts) AS purchase_date,
    current_timestamp() AS etl_timestamp
FROM silver_holding_history shh
INNER JOIN silver_trades st ON shh.hh_t_id = st.trade_id
INNER JOIN gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
INNER JOIN gold_dim_account da ON st.account_id = da.account_id
INNER JOIN gold_dim_security ds ON st.symbol = ds.symbol
WHERE shh.batch_id = ${var.batch_id}
  AND shh.is_current = true
  AND st.is_current = true;

-- gold_fact_watches: From silver_watch_history
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
WHERE swh.batch_id = ${var.batch_id}
  AND swh.is_current = true;

-- ============================================================================
-- Other Gold Tables
-- ============================================================================

-- gold_financials: From silver_financials
INSERT INTO gold_financials
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
    diluted_sh_out,
    current_timestamp() AS etl_timestamp
FROM silver_financials
WHERE batch_id = ${var.batch_id};

-- gold_prospect: From silver_prospect
INSERT INTO gold_prospect
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
    number_credit_cards,
    net_worth,
    current_timestamp() AS etl_timestamp
FROM silver_prospect
WHERE batch_id = ${var.batch_id};
