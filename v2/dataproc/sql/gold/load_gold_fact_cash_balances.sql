DROP TABLE IF EXISTS __CATALOG__.__SCHEMA__.gold_fact_cash_balances;
CREATE TABLE __CATALOG__.__SCHEMA__.gold_fact_cash_balances AS
SELECT 
    dd.sk_date_id,
    da.sk_account_id,
    dc.sk_customer_id,
    sct.ct_ca_id AS account_id,
    SUM(sct.ct_amt) AS cash_balance,
    COUNT(*) AS transaction_count,
    current_timestamp() AS etl_timestamp
FROM __CATALOG__.__SCHEMA__.silver_cash_transaction sct
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_date dd ON DATE(sct.ct_dts) = dd.date_value
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_account da ON TRIM(CAST(sct.ct_ca_id AS STRING)) = TRIM(CAST(da.account_id AS STRING))
INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc ON TRIM(CAST(da.customer_id AS STRING)) = TRIM(CAST(dc.customer_id AS STRING))
WHERE sct.batch_id = __BATCH_ID__
  AND sct.is_current = true
GROUP BY dd.sk_date_id, da.sk_account_id, dc.sk_customer_id, sct.ct_ca_id
