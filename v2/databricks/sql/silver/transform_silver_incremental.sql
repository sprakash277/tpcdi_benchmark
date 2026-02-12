-- ============================================================================
-- TPC-DI v2: Silver Layer - Incremental Transformations (Batch 2+)
-- ============================================================================
-- Transforms Bronze raw data into Silver with SCD Type 2 MERGE logic
-- Batch 2+: Incremental load (MERGE for SCD Type 2 tables)
-- ============================================================================

-- Set variables
-- SET var.batch_id = 2;  -- Change for Batch 3, 4, etc.

-- ============================================================================
-- Brokerage Data: Parse Customer.txt and Account.txt (Batch 2+)
-- ============================================================================

-- silver_customers: Parse Customer.txt with SCD Type 2 MERGE
-- Format: CDC_FLAG|CDC_DSN|C_ID|C_TAX_ID|C_ST_ID|C_L_NAME|...
USE CATALOG __CATALOG__;
USE SCHEMA __SCHEMA__;


WITH incoming_customers AS (
    SELECT 
        monotonically_increasing_id() AS sk_customer_id,
        CAST(split(raw_line, '__PIPE__')[2] AS BIGINT) AS customer_id,  -- Skip CDC_FLAG, CDC_DSN
        split(raw_line, '__PIPE__')[3] AS tax_id,
        split(raw_line, '__PIPE__')[4] AS status,
        split(raw_line, '__PIPE__')[5] AS last_name,
        split(raw_line, '__PIPE__')[6] AS first_name,
        split(raw_line, '__PIPE__')[7] AS middle_name,
        split(raw_line, '__PIPE__')[8] AS gender,
        CAST(split(raw_line, '__PIPE__')[9] AS INT) AS tier,
        CAST(split(raw_line, '__PIPE__')[10] AS DATE) AS dob,
        split(raw_line, '__PIPE__')[11] AS address_line1,
        split(raw_line, '__PIPE__')[12] AS address_line2,
        split(raw_line, '__PIPE__')[13] AS postal_code,
        split(raw_line, '__PIPE__')[14] AS city,
        split(raw_line, '__PIPE__')[15] AS state_prov,
        split(raw_line, '__PIPE__')[16] AS country,
        split(raw_line, '__PIPE__')[17] AS email1,
        split(raw_line, '__PIPE__')[18] AS email2,
        split(raw_line, '__PIPE__')[19] AS local_tax_id,
        split(raw_line, '__PIPE__')[20] AS national_tax_id,
        split(raw_line, '__PIPE__')[0] AS cdc_flag,  -- I=Insert, U=Update, D=Delete
        CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS cdc_dsn,  -- Change timestamp
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_customer
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '__PIPE__')) >= 21
),
-- Close existing current records that have updates
updates_to_close AS (
    SELECT 
        customer_id,
        MIN(cdc_dsn) AS new_effective_date
    FROM incoming_customers
    WHERE cdc_flag IN ('U', 'D')  -- Updates and deletes
    GROUP BY customer_id
)
MERGE INTO silver_customers AS target
USING updates_to_close AS updates
ON target.customer_id = updates.customer_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date;

-- Insert new versions (I and U records)
INSERT INTO silver_customers
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
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,  -- D = inactive
    cdc_dsn AS effective_date,
    NULL AS end_date,
    batch_id,
    load_timestamp,
    cdc_flag AS record_type
FROM incoming_customers
WHERE cdc_flag IN ('I', 'U');  -- Insert new and updated versions (not D-only)

-- silver_accounts: Parse Account.txt with SCD Type 2 MERGE
-- Format: CDC_FLAG|CDC_DSN|CA_ID|CA_B_ID|CA_C_ID|CA_NAME|CA_TAX_ST|CA_ST_ID
WITH incoming_accounts AS (
    SELECT 
        CAST(split(raw_line, '__PIPE__')[2] AS BIGINT) AS account_id,
        CAST(split(raw_line, '__PIPE__')[3] AS BIGINT) AS broker_id,
        CAST(split(raw_line, '__PIPE__')[4] AS BIGINT) AS customer_id,
        split(raw_line, '__PIPE__')[5] AS account_name,
        CAST(split(raw_line, '__PIPE__')[6] AS INT) AS tax_status,
        split(raw_line, '__PIPE__')[7] AS status_id,
        split(raw_line, '__PIPE__')[0] AS cdc_flag,
        CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_account
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '__PIPE__')) >= 8
),
updates_to_close AS (
    SELECT 
        account_id,
        MIN(cdc_dsn) AS new_effective_date
    FROM incoming_accounts
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY account_id
)
MERGE INTO silver_accounts AS target
USING updates_to_close AS updates
ON target.account_id = updates.account_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date;

INSERT INTO silver_accounts
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
WHERE cdc_flag IN ('I', 'U');

-- ============================================================================
-- Transaction Data: Incremental (with CDC columns)
-- ============================================================================

-- silver_trades: Parse Trade.txt (18 columns incremental: +CDC_FLAG, +CDC_DSN)
WITH incoming_trades AS (
    SELECT 
        CAST(split(raw_line, '__PIPE__')[2] AS BIGINT) AS trade_id,  -- Skip CDC_FLAG, CDC_DSN
        CAST(split(raw_line, '__PIPE__')[3] AS TIMESTAMP) AS trade_dts,
        split(raw_line, '__PIPE__')[4] AS status_id,
        split(raw_line, '__PIPE__')[5] AS trade_type_id,
        CAST(split(raw_line, '__PIPE__')[6] AS BOOLEAN) AS is_cash,
        split(raw_line, '__PIPE__')[7] AS symbol,
        CAST(split(raw_line, '__PIPE__')[8] AS INT) AS quantity,
        CAST(split(raw_line, '__PIPE__')[9] AS DOUBLE) AS bid_price,
        CAST(split(raw_line, '__PIPE__')[10] AS BIGINT) AS account_id,
        split(raw_line, '__PIPE__')[11] AS exec_name,
        CAST(split(raw_line, '__PIPE__')[12] AS DOUBLE) AS trade_price,
        CAST(split(raw_line, '__PIPE__')[13] AS DOUBLE) AS charge,
        CAST(split(raw_line, '__PIPE__')[14] AS DOUBLE) AS commission,
        CAST(split(raw_line, '__PIPE__')[15] AS DOUBLE) AS tax,
        split(raw_line, '__PIPE__')[0] AS cdc_flag,
        CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_trade
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '__PIPE__')) = 18  -- Incremental = 18 columns
),
updates_to_close AS (
    SELECT 
        trade_id,
        MIN(cdc_dsn) AS new_effective_date
    FROM incoming_trades
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY trade_id
)
MERGE INTO silver_trades AS target
USING updates_to_close AS updates
ON target.trade_id = updates.trade_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date;

INSERT INTO silver_trades
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
WHERE cdc_flag IN ('I', 'U');

-- silver_daily_market: Parse DailyMarket.txt (8 columns incremental: +CDC_FLAG, +CDC_DSN)
MERGE INTO silver_daily_market AS target
USING (
    SELECT 
        CONCAT(CAST(split(raw_line, '__PIPE__')[2] AS DATE), '|', split(raw_line, '__PIPE__')[3]) AS dm_key,
        CAST(split(raw_line, '__PIPE__')[2] AS DATE) AS dm_date,
        split(raw_line, '__PIPE__')[3] AS dm_s_symb,
        CAST(split(raw_line, '__PIPE__')[4] AS DOUBLE) AS dm_close,
        CAST(split(raw_line, '__PIPE__')[5] AS DOUBLE) AS dm_high,
        CAST(split(raw_line, '__PIPE__')[6] AS DOUBLE) AS dm_low,
        CAST(split(raw_line, '__PIPE__')[7] AS BIGINT) AS dm_vol,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_daily_market
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '__PIPE__')) = 8  -- Incremental = 8 columns
) AS source
ON target.dm_key = source.dm_key
WHEN MATCHED THEN UPDATE SET
    target.dm_close = source.dm_close,
    target.dm_high = source.dm_high,
    target.dm_low = source.dm_low,
    target.dm_vol = source.dm_vol,
    target.batch_id = source.batch_id,
    target.load_timestamp = source.load_timestamp
WHEN NOT MATCHED THEN INSERT *;

-- silver_cash_transaction: Parse CashTransaction.txt (6 columns incremental)
WITH incoming_cash AS (
    SELECT 
        CONCAT(CAST(split(raw_line, '__PIPE__')[2] AS BIGINT), '|', CAST(split(raw_line, '__PIPE__')[3] AS TIMESTAMP)) AS ct_key,
        CAST(split(raw_line, '__PIPE__')[2] AS BIGINT) AS ct_ca_id,
        CAST(split(raw_line, '__PIPE__')[3] AS TIMESTAMP) AS ct_dts,
        CAST(split(raw_line, '__PIPE__')[4] AS DOUBLE) AS ct_amt,
        split(raw_line, '__PIPE__')[5] AS ct_name,
        split(raw_line, '__PIPE__')[0] AS cdc_flag,
        CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_cash_transaction
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '__PIPE__')) = 6
),
updates_to_close AS (
    SELECT 
        ct_key,
        MIN(cdc_dsn) AS new_effective_date
    FROM incoming_cash
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY ct_key
)
MERGE INTO silver_cash_transaction AS target
USING updates_to_close AS updates
ON target.ct_key = updates.ct_key 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date;

INSERT INTO silver_cash_transaction
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
WHERE cdc_flag IN ('I', 'U');

-- silver_holding_history: Parse HoldingHistory.txt (6 columns incremental)
WITH incoming_holdings AS (
    SELECT 
        CAST(split(raw_line, '__PIPE__')[2] AS BIGINT) AS hh_h_t_id,
        CAST(split(raw_line, '__PIPE__')[3] AS BIGINT) AS hh_t_id,
        CAST(split(raw_line, '__PIPE__')[4] AS INT) AS hh_before_qty,
        CAST(split(raw_line, '__PIPE__')[5] AS INT) AS hh_after_qty,
        split(raw_line, '__PIPE__')[0] AS cdc_flag,
        CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_holding_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '__PIPE__')) = 6
),
updates_to_close AS (
    SELECT 
        hh_h_t_id,
        MIN(cdc_dsn) AS new_effective_date
    FROM incoming_holdings
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY hh_h_t_id
)
MERGE INTO silver_holding_history AS target
USING updates_to_close AS updates
ON target.hh_h_t_id = updates.hh_h_t_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date;

INSERT INTO silver_holding_history
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
WHERE cdc_flag IN ('I', 'U');

-- silver_watch_history: Parse WatchHistory.txt (6 columns incremental)
WITH incoming_watches AS (
    SELECT 
        CONCAT(CAST(split(raw_line, '__PIPE__')[2] AS BIGINT), '|', split(raw_line, '__PIPE__')[3]) AS wh_key,
        CAST(split(raw_line, '__PIPE__')[2] AS BIGINT) AS w_c_id,
        split(raw_line, '__PIPE__')[3] AS w_s_symb,
        CAST(split(raw_line, '__PIPE__')[4] AS TIMESTAMP) AS w_dts,
        split(raw_line, '__PIPE__')[5] AS w_action,
        split(raw_line, '__PIPE__')[0] AS cdc_flag,
        CAST(split(raw_line, '__PIPE__')[1] AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_watch_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '__PIPE__')) = 6
),
updates_to_close AS (
    SELECT 
        wh_key,
        MIN(cdc_dsn) AS new_effective_date
    FROM incoming_watches
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY wh_key
)
MERGE INTO silver_watch_history AS target
USING updates_to_close AS updates
ON target.wh_key = updates.wh_key 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date;

INSERT INTO silver_watch_history
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
WHERE cdc_flag IN ('I', 'U');

-- ============================================================================
-- Other Sources: Prospect (Batch 2+)
-- ============================================================================

-- silver_prospect: Parse Prospect.csv (append for incremental)
INSERT INTO silver_prospect
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
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_prospect
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != '';
