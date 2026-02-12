-- Dataproc: source is temp view _tmp_bronze_date (created by runner from Batch1/Date.txt)
-- Delta + Hive catalog: use DROP then CREATE instead of CREATE OR REPLACE TABLE AS SELECT
DROP TABLE IF EXISTS __DATABASE__.bronze_date;
CREATE TABLE __DATABASE__.bronze_date AS
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Date.txt' AS _source_file
FROM _tmp_bronze_date
WHERE value IS NOT NULL AND value != ''
