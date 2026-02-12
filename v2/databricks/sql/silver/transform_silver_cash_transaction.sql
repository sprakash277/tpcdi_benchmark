CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_cash_transaction AS
SELECT 
    -- Composite Key: Account ID + Transaction Timestamp
    CONCAT(CAST(split_part(raw_line, '|', 1) AS STRING), '|', split_part(raw_line, '|', 2)) AS ct_key,
    CAST(split_part(raw_line, '|', 1) AS BIGINT) AS ct_ca_id,
    CAST(split_part(raw_line, '|', 2) AS TIMESTAMP) AS ct_dts,
    CAST(split_part(raw_line, '|', 3) AS DOUBLE) AS ct_amt,
    split_part(raw_line, '|', 4) AS ct_name,
    TRUE AS is_current,
    CAST(split_part(raw_line, '|', 2) AS TIMESTAMP) AS effective_date,
    CAST(NULL AS TIMESTAMP) AS end_date, -- Explicitly cast to avoid VOID type
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp,
    'SBATCH' AS record_type
FROM __CATALOG__.__SCHEMA__.bronze_cash_transaction
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  -- Use double backslash to escape pipe
  AND size(split(raw_line, '\\|')) = 4
