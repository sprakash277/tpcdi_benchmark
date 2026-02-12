DROP TABLE IF EXISTS __DATABASE__.bronze_time;
CREATE TABLE __DATABASE__.bronze_time AS
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Time.txt' AS _source_file
FROM _tmp_bronze_time
WHERE value IS NOT NULL AND value != ''
