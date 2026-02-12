-- TPC-DI v2: Gold incremental - gold_dim_messages (late-arriving trade alerts, Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT 
    current_timestamp() AS message_timestamp,
    __BATCH_ID__ AS batch_id,
    'FactTrade' AS originating_table,
    CONCAT('Late-arriving trade: TradeID=', st.trade_id, ' AccountID=', st.account_id) AS message_text,
    'Alert' AS message_type,
    'Gold_FactTrade_Load' AS component_name,
    'Warning' AS severity
FROM __CATALOG__.__SCHEMA__.silver_trades st
LEFT JOIN __CATALOG__.__SCHEMA__.gold_dim_account da ON st.account_id = da.account_id
WHERE st.batch_id = __BATCH_ID__
  AND st.is_current = true
  AND da.account_id IS NULL;
