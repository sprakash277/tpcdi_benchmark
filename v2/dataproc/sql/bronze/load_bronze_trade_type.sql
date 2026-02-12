DROP TABLE IF EXISTS __DATABASE__.bronze_trade_type;
CREATE TABLE __DATABASE__.bronze_trade_type AS
SELECT 
    value AS raw_line,
    __BATCH_ID__ AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'TradeType.txt' AS _source_file
FROM _tmp_bronze_trade_type
WHERE value IS NOT NULL AND value != ''
