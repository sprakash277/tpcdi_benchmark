DROP TABLE IF EXISTS __DATABASE__.bronze_watch_history;
CREATE TABLE __DATABASE__.bronze_watch_history AS
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'WatchHistory.txt' AS _source_file
FROM _tmp_bronze_watch_history
WHERE value IS NOT NULL AND value != ''
