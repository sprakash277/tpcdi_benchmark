INSERT INTO __DATABASE__.bronze_account (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT value AS raw_line, __BATCH_ID__ AS _batch_id, current_timestamp() AS _load_timestamp, 'Account.txt' AS _source_file
FROM _tmp_bronze_account WHERE value IS NOT NULL AND value != '';
