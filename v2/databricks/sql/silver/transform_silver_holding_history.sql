CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_holding_history AS
SELECT 
    CAST(split_part(raw_line, '|', 1) AS BIGINT) AS hh_h_t_id,
    CAST(split_part(raw_line, '|', 2) AS BIGINT) AS hh_t_id,
    CAST(split_part(raw_line, '|', 3) AS INT) AS hh_before_qty,
    CAST(split_part(raw_line, '|', 4) AS INT) AS hh_after_qty,
    TRUE AS is_current,
    -- Using a fixed start date for historical Batch 1 is often preferred
    CAST('1970-01-01' AS TIMESTAMP) AS effective_date,
    CAST(NULL AS TIMESTAMP) AS end_date, -- Explicitly typed to avoid VOID errors
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp,
    'SBATCH' AS record_type
FROM __CATALOG__.__SCHEMA__.bronze_holding_history
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  AND size(split(raw_line, '|')) = 4
