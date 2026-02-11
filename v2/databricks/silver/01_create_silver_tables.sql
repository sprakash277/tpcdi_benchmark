-- ============================================================================
-- TPC-DI v2: Silver Layer - Databricks
-- ============================================================================
-- Silver Layer: Cleaned, typed, and conformed data
-- SCD Type 2 for dimensions: is_current, effective_date, end_date
-- ============================================================================

-- Set catalog and schema (adjust as needed)
-- USE CATALOG tpcdi_catalog;
-- USE SCHEMA silver_schema;

-- ============================================================================
-- Key Dimensions (SCD Type 2)
-- ============================================================================

-- silver_customers: Customer dimension with SCD Type 2
CREATE TABLE IF NOT EXISTS silver_customers (
    -- Surrogate Key
    sk_customer_id BIGINT,
    
    -- Business Key
    customer_id BIGINT NOT NULL,
    
    -- Attributes
    tax_id STRING,
    status STRING,
    last_name STRING,
    first_name STRING,
    middle_name STRING,
    gender STRING,
    tier INT,
    dob DATE,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state_prov STRING,
    country STRING,
    email1 STRING,
    email2 STRING,
    local_tax_id STRING,
    national_tax_id STRING,
    
    -- SCD Type 2 Columns
    is_current BOOLEAN NOT NULL,
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    
    -- Metadata
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    record_type STRING  -- I=Insert, U=Update, D=Delete
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_accounts: Account dimension with SCD Type 2
CREATE TABLE IF NOT EXISTS silver_accounts (
    -- Business Key
    account_id BIGINT NOT NULL,
    
    -- Attributes
    broker_id BIGINT,
    customer_id BIGINT NOT NULL,
    account_name STRING,
    tax_status INT,
    status_id STRING,
    
    -- SCD Type 2 Columns
    is_current BOOLEAN NOT NULL,
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    
    -- Metadata
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    record_type STRING  -- I=Insert, U=Update, D=Delete
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Market Data (Split from FINWIRE)
-- ============================================================================

-- silver_companies: Company records from FINWIRE (CMP)
CREATE TABLE IF NOT EXISTS silver_companies (
    sk_company_id BIGINT,
    company_id STRING NOT NULL,  -- CIK
    company_name STRING,
    industry_id STRING,
    sp_rating STRING,
    status STRING,
    founding_date DATE,
    ceo_name STRING,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state_province STRING,
    country STRING,
    description STRING,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_securities: Security records from FINWIRE (SEC)
CREATE TABLE IF NOT EXISTS silver_securities (
    symbol STRING NOT NULL,
    issue_type STRING,
    status STRING,
    name STRING,
    ex_id STRING,
    sh_out BIGINT,
    first_trade_date DATE,
    first_trade_exchg STRING,
    dividend DOUBLE,
    co_name_or_cik STRING,  -- Company reference
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_financials: Financial records from FINWIRE (FIN)
CREATE TABLE IF NOT EXISTS silver_financials (
    co_name_or_cik STRING NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    qtr_start_date DATE,
    posting_date DATE,
    revenue DOUBLE,
    earnings DOUBLE,
    eps DOUBLE,
    diluted_eps DOUBLE,
    margin DOUBLE,
    inventory DOUBLE,
    assets DOUBLE,
    liabilities DOUBLE,
    sh_out BIGINT,
    diluted_sh_out BIGINT,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (co_name_or_cik, year, quarter)  -- Composite key for MERGE
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Transaction Data
-- ============================================================================

-- silver_trades: Trade transactions with SCD Type 2
CREATE TABLE IF NOT EXISTS silver_trades (
    trade_id BIGINT NOT NULL,
    trade_dts TIMESTAMP NOT NULL,
    status_id STRING,
    trade_type_id STRING,
    is_cash BOOLEAN,
    symbol STRING,
    quantity INT,
    bid_price DOUBLE,
    account_id BIGINT NOT NULL,
    exec_name STRING,
    trade_price DOUBLE,
    charge DOUBLE,
    commission DOUBLE,
    tax DOUBLE,
    
    -- SCD Type 2 Columns
    is_current BOOLEAN NOT NULL,
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    
    -- Metadata
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    record_type STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_daily_market: Daily market data
CREATE TABLE IF NOT EXISTS silver_daily_market (
    dm_key STRING NOT NULL,  -- Composite: dm_date + dm_s_symb
    dm_date DATE NOT NULL,
    dm_s_symb STRING NOT NULL,
    dm_close DOUBLE,
    dm_high DOUBLE,
    dm_low DOUBLE,
    dm_vol BIGINT,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_cash_transaction: Cash transactions
CREATE TABLE IF NOT EXISTS silver_cash_transaction (
    ct_key STRING NOT NULL,  -- Composite: ct_ca_id + ct_dts
    ct_ca_id BIGINT NOT NULL,  -- Account ID
    ct_dts TIMESTAMP NOT NULL,
    ct_amt DOUBLE,
    ct_name STRING,
    
    -- SCD Type 2 Columns
    is_current BOOLEAN NOT NULL,
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    
    -- Metadata
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    record_type STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_holding_history: Holding history
CREATE TABLE IF NOT EXISTS silver_holding_history (
    hh_h_t_id BIGINT NOT NULL,  -- Holding history trade ID
    hh_t_id BIGINT,  -- Trade ID (join to silver_trades)
    hh_before_qty INT,
    hh_after_qty INT,
    
    -- SCD Type 2 Columns
    is_current BOOLEAN NOT NULL,
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    
    -- Metadata
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    record_type STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_watch_history: Watch list history
CREATE TABLE IF NOT EXISTS silver_watch_history (
    wh_key STRING NOT NULL,  -- Composite: w_c_id + w_s_symb
    w_c_id BIGINT NOT NULL,  -- Customer ID
    w_s_symb STRING NOT NULL,  -- Security symbol
    w_dts TIMESTAMP NOT NULL,
    w_action STRING,
    
    -- SCD Type 2 Columns
    is_current BOOLEAN NOT NULL,
    effective_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    
    -- Metadata
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL,
    record_type STRING
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Reference Data
-- ============================================================================

-- silver_date: Date dimension (parsed from bronze_date)
CREATE TABLE IF NOT EXISTS silver_date (
    sk_date_id INT NOT NULL,
    date_value DATE NOT NULL,
    date_desc STRING,
    calendar_year_id INT,
    calendar_year_desc STRING,
    calendar_qtr_id INT,
    calendar_qtr_desc STRING,
    calendar_month_id INT,
    calendar_month_desc STRING,
    calendar_week_id INT,
    calendar_week_desc STRING,
    day_of_week_num INT,
    day_of_week_desc STRING,
    fiscal_year_id INT,
    fiscal_year_desc STRING,
    fiscal_qtr_id INT,
    fiscal_qtr_desc STRING,
    holiday_flag BOOLEAN,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_time: Time dimension (parsed from bronze_time)
CREATE TABLE IF NOT EXISTS silver_time (
    sk_time_id INT NOT NULL,
    time_value STRING NOT NULL,
    hour_id INT,
    hour_desc STRING,
    minute_id INT,
    minute_desc STRING,
    second_id INT,
    second_desc STRING,
    market_hours_flag BOOLEAN,
    office_hours_flag BOOLEAN,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_status_type: Status type reference
CREATE TABLE IF NOT EXISTS silver_status_type (
    st_id STRING NOT NULL,
    st_name STRING,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_trade_type: Trade type reference
CREATE TABLE IF NOT EXISTS silver_trade_type (
    tt_id STRING NOT NULL,
    tt_name STRING,
    tt_is_sell BOOLEAN,
    tt_is_mrkt BOOLEAN,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_industry: Industry reference
CREATE TABLE IF NOT EXISTS silver_industry (
    in_id STRING NOT NULL,
    in_name STRING,
    in_sc_id STRING,  -- Sector ID
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- silver_tax_rate: Tax rate reference
CREATE TABLE IF NOT EXISTS silver_tax_rate (
    tx_id STRING NOT NULL,
    tx_name STRING,
    tx_rate DOUBLE,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Other Sources
-- ============================================================================

-- silver_prospect: Prospect data
CREATE TABLE IF NOT EXISTS silver_prospect (
    agency_id STRING NOT NULL,
    last_name STRING,
    first_name STRING,
    middle_initial STRING,
    gender STRING,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state STRING,
    country STRING,
    phone STRING,
    income INT,
    number_cars INT,
    number_children INT,
    marital_status STRING,
    age INT,
    credit_rating INT,
    own_or_rent_flag STRING,
    employer STRING,
    number_credit_cards INT,
    net_worth INT,
    batch_id INT NOT NULL,
    load_timestamp TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
