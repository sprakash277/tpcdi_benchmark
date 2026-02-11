CREATE OR REPLACE TABLE __CATALOG__.__SCHEMA__.silver_securities AS
SELECT 
    TRIM(substring(raw_line, 19, 15)) AS symbol,
    TRIM(substring(raw_line, 34, 6)) AS issue_type,
    TRIM(substring(raw_line, 40, 4)) AS status,
    TRIM(substring(raw_line, 44, 70)) AS name,
    TRIM(substring(raw_line, 114, 6)) AS ex_id,
    CAST(TRIM(substring(raw_line, 120, 13)) AS BIGINT) AS sh_out,
    try_to_date(substring(raw_line, 133, 8), 'yyyyMMdd') AS first_trade_date,
    TRIM(substring(raw_line, 141, 8)) AS first_trade_exchg,
    CAST(TRIM(substring(raw_line, 149, 12)) AS DOUBLE) AS dividend,
    TRIM(substring(raw_line, 161, 60)) AS co_name_or_cik,
    _batch_id AS batch_id,
    current_timestamp() AS load_timestamp
FROM __CATALOG__.__SCHEMA__.bronze_finwire
WHERE _batch_id = __BATCH_ID__
  AND substring(raw_line, 16, 3) = 'SEC'
  AND length(raw_line) >= 220
