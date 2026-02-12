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
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS customer_id,  -- Skip CDC_FLAG, CDC_DSN
        split_part(raw_line, '|', 4) AS tax_id,
        split_part(raw_line, '|', 5) AS status,
        split_part(raw_line, '|', 6) AS last_name,
        split_part(raw_line, '|', 7) AS first_name,
        split_part(raw_line, '|', 8) AS middle_name,
        split_part(raw_line, '|', 9) AS gender,
        try_cast(split_part(raw_line, '|', 10) AS INT) AS tier,
        try_cast(split_part(raw_line, '|', 11) AS DATE) AS dob,
        split_part(raw_line, '|', 12) AS address_line1,
        split_part(raw_line, '|', 13) AS address_line2,
        split_part(raw_line, '|', 14) AS postal_code,
        split_part(raw_line, '|', 15) AS city,
        split_part(raw_line, '|', 16) AS state_prov,
        split_part(raw_line, '|', 17) AS country,
        split_part(raw_line, '|', 18) AS email1,
        split_part(raw_line, '|', 19) AS email2,
        split_part(raw_line, '|', 20) AS local_tax_id,
        split_part(raw_line, '|', 21) AS national_tax_id,
        split_part(raw_line, '|', 1) AS cdc_flag,  -- I=Insert, U=Update, D=Delete
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,  -- Change timestamp
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_customer
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) >= 21
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
USING updates_to_close AS src
ON target.customer_id = src.customer_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = src.new_effective_date;

-- Insert new versions (I and U records) - CTE repeated so INSERT runs as separate statement
WITH incoming_customers AS (
    SELECT 
        monotonically_increasing_id() AS sk_customer_id,
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS customer_id,
        split_part(raw_line, '|', 4) AS tax_id,
        split_part(raw_line, '|', 5) AS status,
        split_part(raw_line, '|', 6) AS last_name,
        split_part(raw_line, '|', 7) AS first_name,
        split_part(raw_line, '|', 8) AS middle_name,
        split_part(raw_line, '|', 9) AS gender,
        try_cast(split_part(raw_line, '|', 10) AS INT) AS tier,
        try_cast(split_part(raw_line, '|', 11) AS DATE) AS dob,
        split_part(raw_line, '|', 12) AS address_line1,
        split_part(raw_line, '|', 13) AS address_line2,
        split_part(raw_line, '|', 14) AS postal_code,
        split_part(raw_line, '|', 15) AS city,
        split_part(raw_line, '|', 16) AS state_prov,
        split_part(raw_line, '|', 17) AS country,
        split_part(raw_line, '|', 18) AS email1,
        split_part(raw_line, '|', 19) AS email2,
        split_part(raw_line, '|', 20) AS local_tax_id,
        split_part(raw_line, '|', 21) AS national_tax_id,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_customer
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) >= 21
)
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
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    NULL AS end_date,
    batch_id,
    load_timestamp,
    cdc_flag AS record_type
FROM incoming_customers
WHERE cdc_flag IN ('I', 'U');

-- silver_accounts: Parse Account.txt with SCD Type 2 MERGE
-- Format: CDC_FLAG|CDC_DSN|CA_ID|CA_B_ID|CA_C_ID|CA_NAME|CA_TAX_ST|CA_ST_ID
WITH incoming_accounts AS (
    SELECT 
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS account_id,
        try_cast(split_part(raw_line, '|', 4) AS BIGINT) AS broker_id,
        try_cast(split_part(raw_line, '|', 5) AS BIGINT) AS customer_id,
        split_part(raw_line, '|', 6) AS account_name,
        try_cast(split_part(raw_line, '|', 7) AS INT) AS tax_status,
        split_part(raw_line, '|', 8) AS status_id,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_account
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) >= 8
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
USING updates_to_close AS src
ON target.account_id = src.account_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = src.new_effective_date;

WITH incoming_accounts AS (
    SELECT 
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS account_id,
        try_cast(split_part(raw_line, '|', 4) AS BIGINT) AS broker_id,
        try_cast(split_part(raw_line, '|', 5) AS BIGINT) AS customer_id,
        split_part(raw_line, '|', 6) AS account_name,
        try_cast(split_part(raw_line, '|', 7) AS INT) AS tax_status,
        split_part(raw_line, '|', 8) AS status_id,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_account
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) >= 8
)
INSERT INTO silver_accounts
SELECT 
    account_id,
    broker_id,
    customer_id,
    account_name,
    tax_status,
    status_id,
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
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS trade_id,  -- Skip CDC_FLAG, CDC_DSN
        try_cast(split_part(raw_line, '|', 4) AS TIMESTAMP) AS trade_dts,
        split_part(raw_line, '|', 5) AS status_id,
        split_part(raw_line, '|', 6) AS trade_type_id,
        try_cast(split_part(raw_line, '|', 7) AS BOOLEAN) AS is_cash,
        split_part(raw_line, '|', 8) AS symbol,
        try_cast(split_part(raw_line, '|', 9) AS INT) AS quantity,
        try_cast(split_part(raw_line, '|', 10) AS DOUBLE) AS bid_price,
        try_cast(split_part(raw_line, '|', 11) AS BIGINT) AS account_id,
        split_part(raw_line, '|', 12) AS exec_name,
        try_cast(split_part(raw_line, '|', 13) AS DOUBLE) AS trade_price,
        try_cast(split_part(raw_line, '|', 14) AS DOUBLE) AS charge,
        try_cast(split_part(raw_line, '|', 15) AS DOUBLE) AS commission,
        try_cast(split_part(raw_line, '|', 16) AS DOUBLE) AS tax,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_trade
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 18  -- Incremental = 18 columns
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
USING updates_to_close AS src
ON target.trade_id = src.trade_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = src.new_effective_date;

WITH incoming_trades AS (
    SELECT 
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS trade_id,
        try_cast(split_part(raw_line, '|', 4) AS TIMESTAMP) AS trade_dts,
        split_part(raw_line, '|', 5) AS status_id,
        split_part(raw_line, '|', 6) AS trade_type_id,
        try_cast(split_part(raw_line, '|', 7) AS BOOLEAN) AS is_cash,
        split_part(raw_line, '|', 8) AS symbol,
        try_cast(split_part(raw_line, '|', 9) AS INT) AS quantity,
        try_cast(split_part(raw_line, '|', 10) AS DOUBLE) AS bid_price,
        try_cast(split_part(raw_line, '|', 11) AS BIGINT) AS account_id,
        split_part(raw_line, '|', 12) AS exec_name,
        try_cast(split_part(raw_line, '|', 13) AS DOUBLE) AS trade_price,
        try_cast(split_part(raw_line, '|', 14) AS DOUBLE) AS charge,
        try_cast(split_part(raw_line, '|', 15) AS DOUBLE) AS commission,
        try_cast(split_part(raw_line, '|', 16) AS DOUBLE) AS tax,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_trade
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 18
)
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
        CONCAT(try_cast(split_part(raw_line, '|', 3) AS DATE), '|', split_part(raw_line, '|', 4)) AS dm_key,
        try_cast(split_part(raw_line, '|', 3) AS DATE) AS dm_date,
        split_part(raw_line, '|', 4) AS dm_s_symb,
        try_cast(split_part(raw_line, '|', 5) AS DOUBLE) AS dm_close,
        try_cast(split_part(raw_line, '|', 6) AS DOUBLE) AS dm_high,
        try_cast(split_part(raw_line, '|', 7) AS DOUBLE) AS dm_low,
        try_cast(split_part(raw_line, '|', 8) AS BIGINT) AS dm_vol,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_daily_market
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 8  -- Incremental = 8 columns
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
        CONCAT(try_cast(split_part(raw_line, '|', 3) AS BIGINT), '|', try_cast(split_part(raw_line, '|', 4) AS TIMESTAMP)) AS ct_key,
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS ct_ca_id,
        try_cast(split_part(raw_line, '|', 4) AS TIMESTAMP) AS ct_dts,
        try_cast(split_part(raw_line, '|', 5) AS DOUBLE) AS ct_amt,
        split_part(raw_line, '|', 6) AS ct_name,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_cash_transaction
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
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
USING updates_to_close AS src
ON target.ct_key = src.ct_key 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = src.new_effective_date;

WITH incoming_cash AS (
    SELECT 
        CONCAT(try_cast(split_part(raw_line, '|', 3) AS BIGINT), '|', try_cast(split_part(raw_line, '|', 4) AS TIMESTAMP)) AS ct_key,
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS ct_ca_id,
        try_cast(split_part(raw_line, '|', 4) AS TIMESTAMP) AS ct_dts,
        try_cast(split_part(raw_line, '|', 5) AS DOUBLE) AS ct_amt,
        split_part(raw_line, '|', 6) AS ct_name,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_cash_transaction
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
)
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
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS hh_h_t_id,
        try_cast(split_part(raw_line, '|', 4) AS BIGINT) AS hh_t_id,
        try_cast(split_part(raw_line, '|', 5) AS INT) AS hh_before_qty,
        try_cast(split_part(raw_line, '|', 6) AS INT) AS hh_after_qty,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_holding_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
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
USING updates_to_close AS src
ON target.hh_h_t_id = src.hh_h_t_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = src.new_effective_date;

WITH incoming_holdings AS (
    SELECT 
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS hh_h_t_id,
        try_cast(split_part(raw_line, '|', 4) AS BIGINT) AS hh_t_id,
        try_cast(split_part(raw_line, '|', 5) AS INT) AS hh_before_qty,
        try_cast(split_part(raw_line, '|', 6) AS INT) AS hh_after_qty,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_holding_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
)
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
        CONCAT(try_cast(split_part(raw_line, '|', 3) AS BIGINT), '|', split_part(raw_line, '|', 4)) AS wh_key,
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS w_c_id,
        split_part(raw_line, '|', 4) AS w_s_symb,
        try_cast(split_part(raw_line, '|', 5) AS TIMESTAMP) AS w_dts,
        split_part(raw_line, '|', 6) AS w_action,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_watch_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
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
USING updates_to_close AS src
ON target.wh_key = src.wh_key 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = src.new_effective_date;

WITH incoming_watches AS (
    SELECT 
        CONCAT(try_cast(split_part(raw_line, '|', 3) AS BIGINT), '|', split_part(raw_line, '|', 4)) AS wh_key,
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS w_c_id,
        split_part(raw_line, '|', 4) AS w_s_symb,
        try_cast(split_part(raw_line, '|', 5) AS TIMESTAMP) AS w_dts,
        split_part(raw_line, '|', 6) AS w_action,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id,
        current_timestamp() AS load_timestamp
    FROM bronze_watch_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
)
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

-- silver_prospect: Parse Prospect.csv (append for incremental, comma-delimited like batch)
INSERT INTO silver_prospect
SELECT 
    split_part(raw_line, ',', 1) AS agency_id,
    split_part(raw_line, ',', 2) AS last_name,
    split_part(raw_line, ',', 3) AS first_name,
    split_part(raw_line, ',', 4) AS middle_initial,
    split_part(raw_line, ',', 5) AS gender,
    split_part(raw_line, ',', 6) AS address_line1,
    split_part(raw_line, ',', 7) AS address_line2,
    split_part(raw_line, ',', 8) AS postal_code,
    split_part(raw_line, ',', 9) AS city,
    split_part(raw_line, ',', 10) AS state,
    split_part(raw_line, ',', 11) AS country,
    split_part(raw_line, ',', 12) AS phone,
    try_cast(split_part(raw_line, ',', 13) AS INT) AS income,
    try_cast(split_part(raw_line, ',', 14) AS INT) AS number_cars,
    try_cast(split_part(raw_line, ',', 15) AS INT) AS number_children,
    split_part(raw_line, ',', 16) AS marital_status,
    try_cast(split_part(raw_line, ',', 17) AS INT) AS age,
    try_cast(split_part(raw_line, ',', 18) AS INT) AS credit_rating,
    split_part(raw_line, ',', 19) AS own_or_rent_flag,
    split_part(raw_line, ',', 20) AS employer,
    try_cast(split_part(raw_line, ',', 21) AS BOOLEAN) AS is_customer,
    try_cast(split_part(raw_line, ',', 22) AS BIGINT) AS net_worth,
    array_join(
        array_compact(
            array(
                CASE WHEN try_cast(split_part(raw_line, ',', 22) AS BIGINT) > 1000000 OR try_cast(split_part(raw_line, ',', 13) AS INT) > 200000 THEN 'HighValue' ELSE NULL END,
                CASE WHEN try_cast(split_part(raw_line, ',', 17) AS INT) < 25 THEN 'YoungAdult' ELSE NULL END,
                CASE WHEN try_cast(split_part(raw_line, ',', 18) AS INT) > 700 THEN 'HighCredit' ELSE NULL END
            )
        ),
        ','
    ) AS marketing_nameplate,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM bronze_prospect
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != '';
