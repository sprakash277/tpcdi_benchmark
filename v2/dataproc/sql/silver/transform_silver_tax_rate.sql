DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.silver_tax_rate;
CREATE TABLE __CATALOG__.__SCHEMA__.silver_tax_rate AS
SELECT 
    split_part(raw_line, '|', 1) AS tx_id,
    split_part(raw_line, '|', 2) AS tx_name,
    CAST(split_part(raw_line, '|', 3) AS DOUBLE) AS tx_rate,
    __BATCH_ID__ AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_tax_rate
WHERE _batch_id = __BATCH_ID__
  AND raw_line IS NOT NULL
  AND raw_line != ''
