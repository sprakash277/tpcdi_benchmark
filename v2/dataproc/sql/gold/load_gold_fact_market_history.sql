DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_fact_market_history;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_fact_market_history AS
SELECT 
    dd.sk_date_id,
    ds.sk_security_id,
    dc.sk_company_id,
    sdm.dm_date AS market_date,
    sdm.dm_s_symb AS symbol,
    sdm.dm_close AS close_price,
    sdm.dm_high AS high_price,
    sdm.dm_low AS low_price,
    sdm.dm_vol AS volume,
    sdm.batch_id,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_daily_market sdm
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_date dd ON sdm.dm_date = dd.date_value
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds ON TRIM(CAST(sdm.dm_s_symb AS STRING)) = TRIM(CAST(ds.symbol AS STRING))
LEFT JOIN __CATALOG__.__SCHEMA__.gold_dim_company dc ON CAST(ds.company_id AS STRING) = CAST(dc.company_id AS STRING)
WHERE sdm.batch_id = __BATCH_ID__
