-- TPC-DI v2: Silver incremental - silver_watch_history (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

WITH incoming_watches AS (
    SELECT 
        concat(split_part(raw_line, '|', 3), '-', split_part(raw_line, '|', 4)) AS wh_key,
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS w_c_id,
        split_part(raw_line, '|', 4) AS w_s_symb,
        try_cast(split_part(raw_line, '|', 5) AS TIMESTAMP) AS w_dts,
        split_part(raw_line, '|', 6) AS w_action,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id
    FROM __CATALOG__.__SCHEMA__.bronze_watch_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
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
MERGE INTO __CATALOG__.__SCHEMA__.silver_watch_history AS target
USING updates_to_close AS src
ON target.wh_key = src.wh_key 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = src.new_effective_date;

WITH incoming_watches AS (
    SELECT 
        concat(split_part(raw_line, '|', 3), '-', split_part(raw_line, '|', 4)) AS wh_key,
        try_cast(split_part(raw_line, '|', 3) AS BIGINT) AS w_c_id,
        split_part(raw_line, '|', 4) AS w_s_symb,
        try_cast(split_part(raw_line, '|', 5) AS TIMESTAMP) AS w_dts,
        split_part(raw_line, '|', 6) AS w_action,
        split_part(raw_line, '|', 1) AS cdc_flag,
        try_cast(split_part(raw_line, '|', 2) AS TIMESTAMP) AS cdc_dsn,
        __BATCH_ID__ AS batch_id
    FROM __CATALOG__.__SCHEMA__.bronze_watch_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND size(split(raw_line, '|')) = 6
)
INSERT INTO __CATALOG__.__SCHEMA__.silver_watch_history
SELECT 
    wh_key,
    w_c_id,
    w_s_symb,
    w_dts,
    w_action,
    CASE WHEN cdc_flag = 'D' THEN false ELSE true END AS is_current,
    cdc_dsn AS effective_date,
    CAST(NULL AS TIMESTAMP) AS end_date,
    batch_id,
    current_timestamp() AS load_timestamp,
    cdc_flag AS record_type
FROM incoming_watches
WHERE cdc_flag IN ('I', 'U');
