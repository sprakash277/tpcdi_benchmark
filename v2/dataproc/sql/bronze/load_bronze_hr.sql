DROP TABLE IF EXISTS __DATABASE__.bronze_hr;
CREATE TABLE __DATABASE__.bronze_hr AS
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'HR.csv' AS _source_file
FROM _tmp_bronze_hr
WHERE value IS NOT NULL AND value != ''
