-- TPC-DI v2: Silver incremental - silver_accounts (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

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
    FROM __CATALOG__.__SCHEMA__.bronze_account
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) >= 8
),
updates_to_close AS (
    SELECT 
        account_id,
        CAST(MIN(cdc_dsn) AS TIMESTAMP) AS new_effective_date
    FROM incoming_accounts
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY account_id
)
MERGE INTO __CATALOG__.__SCHEMA__.silver_accounts AS target
USING updates_to_close AS src
ON target.account_id = src.account_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = CAST(src.new_effective_date AS TIMESTAMP);

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
    FROM __CATALOG__.__SCHEMA__.bronze_account
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) >= 8
)
INSERT INTO __CATALOG__.__SCHEMA__.silver_accounts
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
