-- TPC-DI v2: Bronze incremental - bronze_customer (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__, __RAW_DATA_PATH__

INSERT INTO __CATALOG__.__SCHEMA__.bronze_customer (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Customer.txt' AS _source_file
FROM read_files('__RAW_DATA_PATH__/Batch__BATCH_ID__/Customer.txt', format => 'text', lineSep => '\n')
WHERE value IS NOT NULL AND value != '';
