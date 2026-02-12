-- TPC-DI v2: Gold incremental - gold_fact_holdings (Batch 2+)
-- SCD Type 2: Join to account and security on date range (version active at trade time). Use CAST for date predicates.
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

MERGE INTO __CATALOG__.__SCHEMA__.gold_fact_holdings AS target
USING (
    SELECT 
        dd.sk_date_id,
        da.sk_account_id,
        ds.sk_security_id,
        shh.hh_after_qty AS quantity,
        st.trade_price AS purchase_price,
        CAST(st.trade_dts AS DATE) AS purchase_date,
        shh.batch_id
    FROM __CATALOG__.__SCHEMA__.silver_holding_history shh
    INNER JOIN __CATALOG__.__SCHEMA__.silver_trades st
        ON shh.hh_t_id = st.trade_id
    INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_date dd
        ON CAST(st.trade_dts AS DATE) = dd.date_value
    INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_account da
        ON st.account_id = da.account_id
        AND st.trade_dts >= da.start_date
        AND (da.end_date IS NULL OR st.trade_dts < da.end_date)
    INNER JOIN __CATALOG__.__SCHEMA__.gold_dim_security ds
        ON st.symbol = ds.symbol
        AND st.trade_dts >= ds.start_date
        AND (ds.end_date IS NULL OR st.trade_dts < ds.end_date)
    WHERE shh.batch_id = __BATCH_ID__
      AND shh.is_current = true
      AND st.is_current = true
) AS source
ON target.sk_account_id = source.sk_account_id
   AND target.sk_security_id = source.sk_security_id
   AND target.sk_date_id = source.sk_date_id
WHEN MATCHED THEN UPDATE SET
    target.quantity = source.quantity,
    target.purchase_price = source.purchase_price,
    target.purchase_date = source.purchase_date,
    target.batch_id = source.batch_id,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    sk_date_id, sk_account_id, sk_security_id, quantity,
    purchase_price, purchase_date, batch_id, etl_timestamp
) VALUES (
    source.sk_date_id, source.sk_account_id, source.sk_security_id, source.quantity,
    source.purchase_price, source.purchase_date, source.batch_id, current_timestamp()
);
