-- ============================================================================
-- TPC-DI v2: Gold Layer - Dataproc
-- ============================================================================
-- Gold Layer: Business-ready star schema per TPC-DI specification
-- Surrogate Keys (SK_*) for stability during incremental updates
-- ============================================================================

-- Set database (adjust as needed)
-- CREATE DATABASE IF NOT EXISTS tpcdi_gold;
-- USE tpcdi_gold;

-- ============================================================================
-- Dimension Tables (Star Schema)
-- ============================================================================

-- gold_dim_customer: Customer dimension (SCD Type 2 in Silver, current only in Gold)
CREATE TABLE IF NOT EXISTS gold_dim_customer (
    sk_customer_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,  -- Natural key
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
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_customer'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_account: Account dimension
CREATE TABLE IF NOT EXISTS gold_dim_account (
    sk_account_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,  -- Natural key
    broker_id BIGINT,
    customer_id BIGINT NOT NULL,
    account_name STRING,
    tax_status INT,
    status_id STRING,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_account'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_broker: Broker dimension (from HR.csv)
CREATE TABLE IF NOT EXISTS gold_dim_broker (
    sk_broker_id BIGINT NOT NULL,
    broker_id BIGINT NOT NULL,  -- Natural key (EmployeeID where job code = broker)
    broker_name STRING,
    branch STRING,
    office STRING,
    phone STRING,
    is_current BOOLEAN NOT NULL,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_broker'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_company: Company dimension (from FINWIRE CMP)
CREATE TABLE IF NOT EXISTS gold_dim_company (
    sk_company_id BIGINT NOT NULL,
    company_id STRING NOT NULL,  -- Natural key (CIK)
    company_name STRING,
    industry_id STRING,
    sector STRING,  -- Derived from industry
    status STRING,
    address_line1 STRING,
    address_line2 STRING,
    postal_code STRING,
    city STRING,
    state_prov STRING,
    country STRING,
    description STRING,
    founding_date DATE,
    ceo_name STRING,
    is_current BOOLEAN NOT NULL,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_broker'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_security: Security dimension (from FINWIRE SEC)
CREATE TABLE IF NOT EXISTS gold_dim_security (
    sk_security_id STRING NOT NULL,  -- Natural key (Symbol)
    security_id STRING NOT NULL,  -- Same as symbol
    symbol STRING NOT NULL,
    issue_type STRING,
    status STRING,
    name STRING,
    exchange_id STRING,
    shares_outstanding BIGINT,
    first_trade_date DATE,
    first_trade_exchange STRING,
    dividend DOUBLE,
    company_id STRING,  -- Reference to DimCompany
    is_current BOOLEAN NOT NULL,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_company'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_date: Date dimension
CREATE TABLE IF NOT EXISTS gold_dim_date (
    sk_date_id INT NOT NULL,
    date_id INT NOT NULL,  -- Same as sk_date_id
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
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_security'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_time: Time dimension (hour-level)
CREATE TABLE IF NOT EXISTS gold_dim_time (
    sk_time_id INT NOT NULL,
    time_id INT NOT NULL,  -- Same as sk_time_id
    time_value TIME NOT NULL,
    hour_id INT,
    hour_desc STRING,
    minute_id INT,
    minute_desc STRING,
    second_id INT,
    second_desc STRING,
    market_hours_flag BOOLEAN,
    office_hours_flag BOOLEAN,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_date'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_trade_type: Trade type reference
CREATE TABLE IF NOT EXISTS gold_dim_trade_type (
    sk_trade_type_id STRING NOT NULL,
    trade_type_id STRING NOT NULL,
    trade_type_code STRING NOT NULL,  -- Same as trade_type_id
    trade_type_name STRING,
    is_sell BOOLEAN,
    is_market BOOLEAN,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_time'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_status_type: Status type reference
CREATE TABLE IF NOT EXISTS gold_dim_status_type (
    sk_status_type_id STRING NOT NULL,
    status_type_id STRING NOT NULL,
    status_type_code STRING NOT NULL,  -- Same as status_type_id
    status_type_name STRING,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_trade_type'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_industry: Industry reference
CREATE TABLE IF NOT EXISTS gold_dim_industry (
    sk_industry_id STRING NOT NULL,
    industry_id STRING NOT NULL,
    industry_name STRING,
    sector_id STRING,
    sector_name STRING,  -- Derived or lookup
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_status_type'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_dim_trade: Trade dimension (per spec)
CREATE TABLE IF NOT EXISTS gold_dim_trade (
    sk_trade_id BIGINT NOT NULL,
    trade_id BIGINT NOT NULL,  -- Natural key
    trade_dts TIMESTAMP NOT NULL,
    trade_status STRING,
    trade_type STRING,
    is_cash BOOLEAN,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_industry'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Fact Tables (Star Schema)
-- ============================================================================

-- gold_fact_trade: Trade fact table
CREATE TABLE IF NOT EXISTS gold_fact_trade (
    sk_trade_id BIGINT NOT NULL,
    sk_date_id INT NOT NULL,
    sk_time_id INT,
    sk_customer_id BIGINT NOT NULL,
    sk_account_id BIGINT NOT NULL,
    sk_security_id STRING NOT NULL,
    sk_trade_type_id STRING NOT NULL,
    trade_id BIGINT NOT NULL,
    trade_dts TIMESTAMP NOT NULL,
    trade_price DOUBLE,
    trade_quantity INT,
    trade_amount DOUBLE,
    commission DOUBLE,
    charge DOUBLE,
    tax DOUBLE,
    status_id STRING,
    is_cash BOOLEAN,
    exec_name STRING,
    batch_id INT NOT NULL,
    late_arriving_flag BOOLEAN,  -- True if trade arrived before account/customer
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_dim_trade'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_fact_market_history: Market history fact
CREATE TABLE IF NOT EXISTS gold_fact_market_history (
    sk_date_id INT NOT NULL,
    sk_security_id STRING NOT NULL,
    sk_company_id BIGINT,
    market_date DATE NOT NULL,
    symbol STRING NOT NULL,
    close_price DOUBLE,
    high_price DOUBLE,
    low_price DOUBLE,
    volume BIGINT,
    batch_id INT NOT NULL,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_fact_trade'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_fact_cash_balances: Cash balances fact (aggregated)
CREATE TABLE IF NOT EXISTS gold_fact_cash_balances (
    sk_date_id INT NOT NULL,
    sk_account_id BIGINT NOT NULL,
    sk_customer_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL,
    cash_balance DOUBLE,  -- Sum of CT_AMT by account/date
    transaction_count BIGINT,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_fact_market_history'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_fact_holdings: Holdings fact
CREATE TABLE IF NOT EXISTS gold_fact_holdings (
    sk_date_id INT NOT NULL,
    sk_account_id BIGINT NOT NULL,
    sk_security_id STRING NOT NULL,
    account_id BIGINT NOT NULL,
    symbol STRING NOT NULL,
    quantity BIGINT,
    purchase_price DOUBLE,
    purchase_date DATE,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_fact_cash_balances'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_fact_watches: Watches fact
CREATE TABLE IF NOT EXISTS gold_fact_watches (
    sk_customer_id BIGINT NOT NULL,
    sk_security_id STRING NOT NULL,
    customer_id BIGINT NOT NULL,
    symbol STRING NOT NULL,
    watch_date TIMESTAMP NOT NULL,
    watch_action STRING,
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_fact_holdings'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Other Gold Tables
-- ============================================================================

-- gold_financials: Financial reporting (SCD Type 1 - latest only)
CREATE TABLE IF NOT EXISTS gold_financials (
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
    etl_timestamp TIMESTAMP NOT NULL,
    PRIMARY KEY (co_name_or_cik, year, quarter)
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_fact_watches'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- gold_prospect: Prospect table (from Prospect.csv)
CREATE TABLE IF NOT EXISTS gold_prospect (
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
    etl_timestamp TIMESTAMP NOT NULL
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_financials'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================================
-- Audit Table: DI_Messages (TPC-DI Spec Requirement)
-- ============================================================================

-- gold_dim_messages: Audit/logging table for data quality and referential integrity
CREATE TABLE IF NOT EXISTS gold_dim_messages (
    message_timestamp TIMESTAMP NOT NULL,
    batch_id INT NOT NULL,
    originating_table STRING NOT NULL,  -- Source table (e.g., 'FactTrade', 'DimCustomer')
    message_text STRING NOT NULL,
    message_type STRING NOT NULL,  -- 'Alert', 'Reject', 'Info'
    component_name STRING,  -- Component that generated the message (e.g., 'Silver_Customer_Validation')
    severity STRING  -- 'Alert', 'Reject', 'Warning', 'Info'
) USING DELTA
LOCATION 'gs://YOUR_BUCKET/tpcdi/gold/gold_prospect'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
