DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.silver_watch_history;
CREATE TABLE __CATALOG__.__SCHEMA__.silver_watch_history AS
SELECT 
    -- Composite Key: Essential for Batch 2/3 MERGE operations
    concat(split_part(raw_line, '|', 1), '-', split_part(raw_line, '|', 2)) AS wh_key,
    CAST(split_part(raw_line, '|', 1) AS BIGINT) AS w_c_id,
    split_part(raw_line, '|', 2) AS w_s_symb,
    CAST(split_part(raw_line, '|', 3) AS TIMESTAMP) AS w_dts,
    split_part(raw_line, '|', 4) AS w_action,
    TRUE AS is_current,
    CAST(split_part(raw_line, '|', 3) AS TIMESTAMP) AS effective_date,
    CAST(NULL AS TIMESTAMP) AS end_date, -- Hardened against VOID type errors
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp,
    'SBATCH' AS record_type
FROM __CATALOG__.__SCHEMA__.bronze_watch_history
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
  -- FIXED: Escaped pipe for correct regex splitting
  AND size(split(raw_line, '\\|')) = 4
