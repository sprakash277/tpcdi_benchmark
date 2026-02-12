INSERT INTO __DATABASE__.bronze_watch_history (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT value AS raw_line, __BATCH_ID__ AS _batch_id, current_timestamp() AS _load_timestamp, 'WatchHistory.txt' AS _source_file
FROM _tmp_bronze_watch_history WHERE value IS NOT NULL AND value != '';
