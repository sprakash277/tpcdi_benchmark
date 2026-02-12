-- TPC-DI v2: Gold incremental - gold_fact_cash_balances (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

MERGE INTO __CATALOG__.__SCHEMA__.gold_fact_cash_balances AS target
USING (
    SELECT 
        dd.sk_date_id,
        da.sk_account_id,
        dc.sk_customer_id,
        sct.ct_ca_id AS account_id,
        SUM(sct.ct_amt) AS cash_balance,
        COUNT(*) AS transaction_count
    FROM __CATALOG__.__SCHEMA__.silver_cash_transaction sct
    INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_date dd ON DATE(sct.ct_dts) = dd.date_value
    INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_account da ON sct.ct_ca_id = da.account_id
    INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_customer dc ON da.customer_id = dc.customer_id
    WHERE sct.batch_id = __BATCH_ID__
      AND sct.is_current = true
    GROUP BY dd.sk_date_id, da.sk_account_id, dc.sk_customer_id, sct.ct_ca_id
) AS source
ON target.sk_date_id = source.sk_date_id
   AND target.sk_account_id = source.sk_account_id
WHEN MATCHED THEN UPDATE SET
    target.cash_balance = source.cash_balance,
    target.transaction_count = source.transaction_count,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
