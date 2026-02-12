CREATE OR REPLACE TABLE __DATABASE__.bronze_prospect AS
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Prospect.csv' AS _source_file
FROM _tmp_bronze_prospect
WHERE value IS NOT NULL AND value != ''
