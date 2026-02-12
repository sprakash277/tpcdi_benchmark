-- TPC-DI v2: Silver incremental - silver_cash_transaction (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

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
    FROM __CATALOG__.__SCHEMA__.bronze_cash_transaction
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
),
updates_to_close AS (
    SELECT 
        ct_key,
        CAST(MIN(cdc_dsn) AS TIMESTAMP) AS new_effective_date
    FROM incoming_cash
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY ct_key
)
MERGE INTO __CATALOG__.__SCHEMA__.silver_cash_transaction AS target
USING updates_to_close AS src
ON target.ct_key = src.ct_key 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = CAST(src.new_effective_date AS TIMESTAMP);

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
    FROM __CATALOG__.__SCHEMA__.bronze_cash_transaction
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
)
INSERT INTO __CATALOG__.__SCHEMA__.silver_cash_transaction
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
