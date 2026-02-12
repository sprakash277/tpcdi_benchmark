DROP TABLE IF EXISTS __DATABASE__.bronze_tax_rate;
CREATE TABLE __DATABASE__.bronze_tax_rate AS
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'TaxRate.txt' AS _source_file
FROM _tmp_bronze_tax_rate
WHERE value IS NOT NULL AND value != ''
