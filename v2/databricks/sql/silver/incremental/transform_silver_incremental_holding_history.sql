-- TPC-DI v2: Silver incremental - silver_holding_history (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

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
    FROM __CATALOG__.__SCHEMA__.bronze_holding_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
),
updates_to_close AS (
    SELECT 
        hh_h_t_id,
        CAST(MIN(cdc_dsn) AS TIMESTAMP) AS new_effective_date
    FROM incoming_holdings
    WHERE cdc_flag IN ('U', 'D')
    GROUP BY hh_h_t_id
)
MERGE INTO __CATALOG__.__SCHEMA__.silver_holding_history AS target
USING updates_to_close AS src
ON target.hh_h_t_id = src.hh_h_t_id 
   AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = CAST(src.new_effective_date AS TIMESTAMP);

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
    FROM __CATALOG__.__SCHEMA__.bronze_holding_history
    WHERE _batch_id = __BATCH_ID__
      AND raw_line IS NOT NULL
      AND raw_line != ''
      AND size(split(raw_line, '|')) = 6
)
INSERT INTO __CATALOG__.__SCHEMA__.silver_holding_history
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
