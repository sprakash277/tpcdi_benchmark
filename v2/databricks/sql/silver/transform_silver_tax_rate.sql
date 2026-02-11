CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_tax_rate AS
SELECT 
    split(raw_line, '__PIPE__')[0] AS tx_id,
    split(raw_line, '__PIPE__')[1] AS tx_name,
    CAST(split(raw_line, '__PIPE__')[2] AS DOUBLE) AS tx_rate,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_tax_rate
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
