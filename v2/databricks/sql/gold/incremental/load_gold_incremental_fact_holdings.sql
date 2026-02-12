-- TPC-DI v2: Gold incremental - gold_fact_holdings (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

MERGE INTO __CATALOG__.__SCHEMA__.gold_fact_holdings AS target
USING (
    SELECT 
        dd.sk_date_id,
        da.sk_account_id,
        ds.sk_security_id,
        st.account_id,
        st.symbol,
        shh.hh_after_qty AS quantity,
        st.trade_price AS purchase_price,
        DATE(st.trade_dts) AS purchase_date
    FROM __CATALOG__.__SCHEMA__.silver_holding_history shh
    INNER JOIN __CATALOG__.__SCHEMA__.silver_trades st ON shh.hh_t_id = st.trade_id
    INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_date dd ON DATE(st.trade_dts) = dd.date_value
    INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_account da ON st.account_id = da.account_id
    INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds ON st.symbol = ds.symbol
    WHERE shh.batch_id = __BATCH_ID__
      AND shh.is_current = true
      AND st.is_current = true
) AS source
ON target.sk_date_id = source.sk_date_id
   AND target.sk_account_id = source.sk_account_id
   AND target.sk_security_id = source.sk_security_id
WHEN MATCHED THEN UPDATE SET
    target.quantity = source.quantity,
    target.purchase_price = source.purchase_price,
    target.purchase_date = source.purchase_date,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT *;
