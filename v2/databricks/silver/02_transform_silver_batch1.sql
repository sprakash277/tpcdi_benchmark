-- ============================================================================
-- TPC-DI v2: Silver Layer - Batch 1 Transformations
-- ============================================================================
-- Transforms Bronze raw data into Silver cleaned, typed tables
-- Batch 1: Historical load (overwrite mode)
-- ============================================================================

-- Set variables
-- SET var.batch_id = 1;

-- ============================================================================
-- Reference Data (Batch 1: Overwrite)
-- ============================================================================

-- silver_date: Parse Date.txt (18 columns pipe-delimited)
USE CATALOG ${var.catalog};
USE SCHEMA ${var.schema};


INSERT OVERWRITE silver_date
SELECT 
    CAST(split(raw_line, '\\|')[0] AS INT) AS sk_date_id,
    CAST(split(raw_line, '\\|')[1] AS DATE) AS date_value,
    split(raw_line, '\\|')[2] AS date_desc,
    CAST(split(raw_line, '\\|')[3] AS INT) AS calendar_year_id,
    split(raw_line, '\\|')[4] AS calendar_year_desc,
    CAST(split(raw_line, '\\|')[5] AS INT) AS calendar_qtr_id,
    split(raw_line, '\\|')[6] AS calendar_qtr_desc,
    CAST(split(raw_line, '\\|')[7] AS INT) AS calendar_month_id,
    split(raw_line, '\\|')[8] AS calendar_month_desc,
    CAST(split(raw_line, '\\|')[9] AS INT) AS calendar_week_id,
    split(raw_line, '\\|')[10] AS calendar_week_desc,
    CAST(split(raw_line, '\\|')[11] AS INT) AS day_of_week_num,
    split(raw_line, '\\|')[12] AS day_of_week_desc,
    CAST(split(raw_line, '\\|')[13] AS INT) AS fiscal_year_id,
    split(raw_line, '\\|')[14] AS fiscal_year_desc,
    CAST(split(raw_line, '\\|')[15] AS INT) AS fiscal_qtr_id,
    split(raw_line, '\\|')[16] AS fiscal_qtr_desc,
    CAST(split(raw_line, '\\|')[17] AS BOOLEAN) AS holiday_flag,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_date
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != '';

-- silver_time: Parse Time.txt (10 columns pipe-delimited)
INSERT OVERWRITE silver_time
SELECT 
    CAST(split(raw_line, '\\|')[0] AS INT) AS sk_time_id,
    split(raw_line, '\\|')[1] AS time_value,
    CAST(split(raw_line, '\\|')[2] AS INT) AS hour_id,
    split(raw_line, '\\|')[3] AS hour_desc,
    CAST(split(raw_line, '\\|')[4] AS INT) AS minute_id,
    split(raw_line, '\\|')[5] AS minute_desc,
    CAST(split(raw_line, '\\|')[6] AS INT) AS second_id,
    split(raw_line, '\\|')[7] AS second_desc,
    CAST(split(raw_line, '\\|')[8] AS BOOLEAN) AS market_hours_flag,
    CAST(split(raw_line, '\\|')[9] AS BOOLEAN) AS office_hours_flag,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_time
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != '';

-- silver_status_type: Parse StatusType.txt (2 columns)
INSERT OVERWRITE silver_status_type
SELECT 
    split(raw_line, '\\|')[0] AS st_id,
    split(raw_line, '\\|')[1] AS st_name,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_status_type
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != '';

-- silver_trade_type: Parse TradeType.txt (4 columns)
INSERT OVERWRITE silver_trade_type
SELECT 
    split(raw_line, '\\|')[0] AS tt_id,
    split(raw_line, '\\|')[1] AS tt_name,
    CAST(split(raw_line, '\\|')[2] AS BOOLEAN) AS tt_is_sell,
    CAST(split(raw_line, '\\|')[3] AS BOOLEAN) AS tt_is_mrkt,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_trade_type
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != '';

-- silver_industry: Parse Industry.txt (3 columns)
INSERT OVERWRITE silver_industry
SELECT 
    split(raw_line, '\\|')[0] AS in_id,
    split(raw_line, '\\|')[1] AS in_name,
    split(raw_line, '\\|')[2] AS in_sc_id,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_industry
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != '';

-- silver_tax_rate: Parse TaxRate.txt (3 columns)
INSERT OVERWRITE silver_tax_rate
SELECT 
    split(raw_line, '\\|')[0] AS tx_id,
    split(raw_line, '\\|')[1] AS tx_name,
    CAST(split(raw_line, '\\|')[2] AS DOUBLE) AS tx_rate,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_tax_rate
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != '';

-- ============================================================================
-- Market Data: Parse FINWIRE (Fixed-Width)
-- ============================================================================

-- silver_companies: Extract CMP records from FINWIRE
INSERT OVERWRITE silver_companies
SELECT 
    monotonically_increasing_id() AS sk_company_id,
    TRIM(substring(raw_line, 79, 10)) AS company_id,  -- CIK
    TRIM(substring(raw_line, 19, 60)) AS company_name,
    TRIM(substring(raw_line, 93, 10)) AS industry_id,
    TRIM(substring(raw_line, 103, 4)) AS sp_rating,
    TRIM(substring(raw_line, 89, 4)) AS status,
    CAST(TRIM(substring(raw_line, 107, 8)) AS DATE) AS founding_date,
    TRIM(substring(raw_line, 115, 15)) AS ceo_name,
    TRIM(substring(raw_line, 130, 45)) AS address_line1,
    TRIM(substring(raw_line, 175, 45)) AS address_line2,
    TRIM(substring(raw_line, 220, 25)) AS postal_code,
    TRIM(substring(raw_line, 245, 25)) AS city,
    TRIM(substring(raw_line, 270, 25)) AS state_province,
    TRIM(substring(raw_line, 295, 25)) AS country,
    TRIM(substring(raw_line, 320, 45)) AS description,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_finwire
WHERE _batch_id = ${var.batch_id}
  AND substring(raw_line, 16, 3) = 'CMP'  -- Record type = CMP
  AND length(raw_line) >= 364;

-- silver_securities: Extract SEC records from FINWIRE
INSERT OVERWRITE silver_securities
SELECT 
    TRIM(substring(raw_line, 19, 15)) AS symbol,
    TRIM(substring(raw_line, 34, 6)) AS issue_type,
    TRIM(substring(raw_line, 40, 10)) AS status,
    TRIM(substring(raw_line, 50, 70)) AS name,
    TRIM(substring(raw_line, 120, 12)) AS ex_id,
    CAST(TRIM(substring(raw_line, 132, 18)) AS BIGINT) AS sh_out,
    CAST(TRIM(substring(raw_line, 150, 16)) AS DATE) AS first_trade_date,
    TRIM(substring(raw_line, 166, 16)) AS first_trade_exchg,
    CAST(TRIM(substring(raw_line, 182, 8)) AS DOUBLE) AS dividend,
    TRIM(substring(raw_line, 190, 60)) AS co_name_or_cik,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_finwire
WHERE _batch_id = ${var.batch_id}
  AND substring(raw_line, 16, 3) = 'SEC'  -- Record type = SEC
  AND length(raw_line) >= 250;

-- silver_financials: Extract FIN records from FINWIRE
INSERT OVERWRITE silver_financials
SELECT 
    TRIM(substring(raw_line, 214, 60)) AS co_name_or_cik,
    CAST(TRIM(substring(raw_line, 19, 4)) AS INT) AS year,
    CAST(TRIM(substring(raw_line, 23, 1)) AS INT) AS quarter,
    CAST(TRIM(substring(raw_line, 24, 8)) AS DATE) AS qtr_start_date,
    CAST(TRIM(substring(raw_line, 34, 8)) AS DATE) AS posting_date,
    CAST(TRIM(substring(raw_line, 51, 17)) AS DOUBLE) AS revenue,
    CAST(TRIM(substring(raw_line, 68, 17)) AS DOUBLE) AS earnings,
    CAST(TRIM(substring(raw_line, 85, 12)) AS DOUBLE) AS eps,
    CAST(TRIM(substring(raw_line, 102, 12)) AS DOUBLE) AS diluted_eps,
    CAST(TRIM(substring(raw_line, 119, 12)) AS DOUBLE) AS margin,
    CAST(TRIM(substring(raw_line, 136, 17)) AS DOUBLE) AS inventory,
    CAST(TRIM(substring(raw_line, 153, 17)) AS DOUBLE) AS assets,
    CAST(TRIM(substring(raw_line, 170, 17)) AS DOUBLE) AS liabilities,
    CAST(TRIM(substring(raw_line, 187, 13)) AS BIGINT) AS sh_out,
    CAST(TRIM(substring(raw_line, 204, 13)) AS BIGINT) AS diluted_sh_out,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_finwire
WHERE _batch_id = ${var.batch_id}
  AND substring(raw_line, 16, 3) = 'FIN'  -- Record type = FIN
  AND length(raw_line) >= 273;

-- ============================================================================
-- Brokerage Data: Parse CustomerMgmt.xml (Batch 1)
-- ============================================================================

-- silver_customers: Extract from CustomerMgmt.xml
-- Note: This assumes XML is parsed using spark-xml or native XML reader
-- Adjust column paths based on your XML parsing method
INSERT OVERWRITE silver_customers
SELECT 
    monotonically_increasing_id() AS sk_customer_id,
    CAST(Customer._C_ID AS BIGINT) AS customer_id,
    Customer._C_TAX_ID AS tax_id,
    Customer._C_ST_ID AS status,
    Customer._C_L_NAME AS last_name,
    Customer._C_F_NAME AS first_name,
    Customer._C_M_NAME AS middle_name,
    Customer._C_GNDR AS gender,
    CAST(Customer._C_TIER AS INT) AS tier,
    CAST(Customer._C_DOB AS DATE) AS dob,
    Customer._C_ADLINE1 AS address_line1,
    Customer._C_ADLINE2 AS address_line2,
    Customer._C_ZIPCODE AS postal_code,
    Customer._C_CITY AS city,
    Customer._C_STATE_PROV AS state_prov,
    Customer._C_CTRY AS country,
    Customer._C_CTRY_1 AS email1,
    Customer._C_CTRY_2 AS email2,
    Customer._C_LOCAL_TAX_ID AS local_tax_id,
    Customer._C_NAT_TX_ID AS national_tax_id,
    -- SCD Type 2: All Batch 1 records are current
    TRUE AS is_current,
    CAST(Customer._C_CTRY_TS AS TIMESTAMP) AS effective_date,  -- Use action timestamp
    NULL AS end_date,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp,
    Customer._C_ACTION AS record_type  -- NEW, UPDCUST, INACT, etc.
FROM bronze_customer_mgmt
LATERAL VIEW explode(Customer) AS Customer
WHERE _batch_id = ${var.batch_id}
  AND raw_xml IS NOT NULL;

-- silver_accounts: Extract from CustomerMgmt.xml
INSERT OVERWRITE silver_accounts
SELECT 
    CAST(Account._CA_ID AS BIGINT) AS account_id,
    CAST(Account._CA_B_ID AS BIGINT) AS broker_id,
    CAST(Account._CA_C_ID AS BIGINT) AS customer_id,
    Account._CA_NAME AS account_name,
    CAST(Account._CA_TAX_ST AS INT) AS tax_status,
    Account._CA_ST_ID AS status_id,
    Account._CA_ACTION AS action_type,
    CAST(Account._CA_ACTION_TS AS TIMESTAMP) AS action_timestamp,
    -- SCD Type 2: All Batch 1 records are current
    TRUE AS is_current,
    CAST(Account._CA_ACTION_TS AS TIMESTAMP) AS effective_date,
    NULL AS end_date,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp,
    Account._CA_ACTION AS record_type  -- NEW, ADDACCT, UPDACCT, CLOSEACCT, etc.
FROM bronze_customer_mgmt
LATERAL VIEW explode(Customer) AS Customer
LATERAL VIEW explode(Customer.Account) AS Account
WHERE _batch_id = ${var.batch_id}
  AND raw_xml IS NOT NULL;

-- ============================================================================
-- Transaction Data (Batch 1)
-- ============================================================================

-- silver_trades: Parse Trade.txt (16 columns historical)
INSERT OVERWRITE silver_trades
SELECT 
    CAST(split(raw_line, '\\|')[0] AS BIGINT) AS trade_id,
    CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS trade_dts,
    split(raw_line, '\\|')[2] AS status_id,
    split(raw_line, '\\|')[3] AS trade_type_id,
    CAST(split(raw_line, '\\|')[4] AS BOOLEAN) AS is_cash,
    split(raw_line, '\\|')[5] AS symbol,
    CAST(split(raw_line, '\\|')[6] AS INT) AS quantity,
    CAST(split(raw_line, '\\|')[7] AS DOUBLE) AS bid_price,
    CAST(split(raw_line, '\\|')[8] AS BIGINT) AS account_id,
    split(raw_line, '\\|')[9] AS exec_name,
    CAST(split(raw_line, '\\|')[10] AS DOUBLE) AS trade_price,
    CAST(split(raw_line, '\\|')[11] AS DOUBLE) AS charge,
    CAST(split(raw_line, '\\|')[12] AS DOUBLE) AS commission,
    CAST(split(raw_line, '\\|')[13] AS DOUBLE) AS tax,
    -- SCD Type 2: All Batch 1 records are current
    TRUE AS is_current,
    CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS effective_date,  -- Use trade_dts
    NULL AS end_date,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type  -- Historical has no record_type
FROM bronze_trade
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\|')) = 16;  -- Historical = 16 columns

-- silver_daily_market: Parse DailyMarket.txt (6 columns historical)
INSERT OVERWRITE silver_daily_market
SELECT 
    CONCAT(CAST(split(raw_line, '\\|')[0] AS DATE), '|', split(raw_line, '\\|')[1]) AS dm_key,
    CAST(split(raw_line, '\\|')[0] AS DATE) AS dm_date,
    split(raw_line, '\\|')[1] AS dm_s_symb,
    CAST(split(raw_line, '\\|')[2] AS DOUBLE) AS dm_close,
    CAST(split(raw_line, '\\|')[3] AS DOUBLE) AS dm_high,
    CAST(split(raw_line, '\\|')[4] AS DOUBLE) AS dm_low,
    CAST(split(raw_line, '\\|')[5] AS BIGINT) AS dm_vol,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_daily_market
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\|')) = 6;  -- Historical = 6 columns

-- silver_cash_transaction: Parse CashTransaction.txt (4 columns historical)
INSERT OVERWRITE silver_cash_transaction
SELECT 
    CONCAT(CAST(split(raw_line, '\\|')[0] AS BIGINT), '|', CAST(split(raw_line, '\\|')[1] AS TIMESTAMP)) AS ct_key,
    CAST(split(raw_line, '\\|')[0] AS BIGINT) AS ct_ca_id,
    CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS ct_dts,
    CAST(split(raw_line, '\\|')[2] AS DOUBLE) AS ct_amt,
    split(raw_line, '\\|')[3] AS ct_name,
    TRUE AS is_current,
    CAST(split(raw_line, '\\|')[1] AS TIMESTAMP) AS effective_date,
    NULL AS end_date,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM bronze_cash_transaction
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\|')) = 4;  -- Historical = 4 columns

-- silver_holding_history: Parse HoldingHistory.txt (4 columns historical)
INSERT OVERWRITE silver_holding_history
SELECT 
    CAST(split(raw_line, '\\|')[0] AS BIGINT) AS hh_h_t_id,
    CAST(split(raw_line, '\\|')[1] AS BIGINT) AS hh_t_id,
    CAST(split(raw_line, '\\|')[2] AS INT) AS hh_before_qty,
    CAST(split(raw_line, '\\|')[3] AS INT) AS hh_after_qty,
    TRUE AS is_current,
    current_timestamp() AS effective_date,
    NULL AS end_date,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM bronze_holding_history
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\|')) = 4;  -- Historical = 4 columns

-- silver_watch_history: Parse WatchHistory.txt (4 columns historical)
INSERT OVERWRITE silver_watch_history
SELECT 
    CONCAT(CAST(split(raw_line, '\\|')[0] AS BIGINT), '|', split(raw_line, '\\|')[1]) AS wh_key,
    CAST(split(raw_line, '\\|')[0] AS BIGINT) AS w_c_id,
    split(raw_line, '\\|')[1] AS w_s_symb,
    CAST(split(raw_line, '\\|')[2] AS TIMESTAMP) AS w_dts,
    split(raw_line, '\\|')[3] AS w_action,
    TRUE AS is_current,
    CAST(split(raw_line, '\\|')[2] AS TIMESTAMP) AS effective_date,
    NULL AS end_date,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp,
    NULL AS record_type
FROM bronze_watch_history
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '\\|')) = 4;  -- Historical = 4 columns

-- ============================================================================
-- Other Sources (Batch 1)
-- ============================================================================

-- silver_prospect: Parse Prospect.csv (23 columns comma-delimited)
INSERT OVERWRITE silver_prospect
SELECT 
    split(raw_line, ',')[0] AS agency_id,
    split(raw_line, ',')[1] AS last_name,
    split(raw_line, ',')[2] AS first_name,
    split(raw_line, ',')[3] AS middle_initial,
    split(raw_line, ',')[4] AS gender,
    split(raw_line, ',')[5] AS address_line1,
    split(raw_line, ',')[6] AS address_line2,
    split(raw_line, ',')[7] AS postal_code,
    split(raw_line, ',')[8] AS city,
    split(raw_line, ',')[9] AS state,
    split(raw_line, ',')[10] AS country,
    split(raw_line, ',')[11] AS phone,
    CAST(split(raw_line, ',')[12] AS INT) AS income,
    CAST(split(raw_line, ',')[13] AS INT) AS number_cars,
    CAST(split(raw_line, ',')[14] AS INT) AS number_children,
    split(raw_line, ',')[15] AS marital_status,
    CAST(split(raw_line, ',')[16] AS INT) AS age,
    CAST(split(raw_line, ',')[17] AS INT) AS credit_rating,
    split(raw_line, ',')[18] AS own_or_rent_flag,
    split(raw_line, ',')[19] AS employer,
    CAST(split(raw_line, ',')[20] AS INT) AS number_credit_cards,
    CAST(split(raw_line, ',')[21] AS INT) AS net_worth,
    ${var.batch_id} AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_prospect
WHERE _batch_id = ${var.batch_id}
  AND raw_line IS NOT NULL
  AND raw_line != '';
