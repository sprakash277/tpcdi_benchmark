-- TPC-DI v2: DQ rules for silver_watch_history (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_watch_history', 'w_c_id NULL', 'Validation', 'Silver_WatchHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_watch_history WHERE batch_id = __BATCH_ID__ AND w_c_id IS NULL LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_watch_history', 'w_s_symb NULL or empty', 'Validation', 'Silver_WatchHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_watch_history WHERE batch_id = __BATCH_ID__ AND (w_s_symb IS NULL OR TRIM(CAST(w_s_symb AS STRING)) = '') LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_watch_history', 'w_action not in (ACTV,CNCL,INAC)', 'Validation', 'Silver_WatchHistory_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_watch_history WHERE batch_id = __BATCH_ID__ AND w_action IS NOT NULL AND TRIM(CAST(w_action AS STRING)) != '' AND TRIM(CAST(w_action AS STRING)) NOT IN ('ACTV', 'CNCL', 'INAC') LIMIT 1) t;
