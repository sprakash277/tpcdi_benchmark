INSERT INTO __DATABASE__.bronze_cash_transaction (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT value AS raw_line, __BATCH_ID__ AS _batch_id, current_timestamp() AS _load_timestamp, 'CashTransaction.txt' AS _source_file
FROM _tmp_bronze_cash_transaction WHERE value IS NOT NULL AND value != '';
