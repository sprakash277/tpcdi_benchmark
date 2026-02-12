-- TPC-DI v2: Gold incremental - gold_dim_security (Batch 2+)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

MERGE INTO __CATALOG__.__SCHEMA__.gold_dim_security AS target
USING (
    SELECT 
        ss.symbol AS sk_security_id,
        ss.symbol AS security_id,
        ss.symbol,
        ss.issue_type,
        ss.status,
        ss.name,
        ss.ex_id AS exchange_id,
        ss.sh_out AS shares_outstanding,
        ss.first_trade_date,
        ss.first_trade_exchg AS first_trade_exchange,
        ss.dividend,
        ss.co_name_or_cik AS company_id
    FROM __CATALOG__.__SCHEMA__.silver_securities ss
    WHERE ss.batch_id = __BATCH_ID__
) AS source
ON target.symbol = source.symbol
WHEN MATCHED THEN UPDATE SET
    target.issue_type = source.issue_type,
    target.status = source.status,
    target.name = source.name,
    target.exchange_id = source.exchange_id,
    target.shares_outstanding = source.shares_outstanding,
    target.first_trade_date = source.first_trade_date,
    target.first_trade_exchange = source.first_trade_exchange,
    target.dividend = source.dividend,
    target.company_id = source.company_id,
    target.is_current = true,
    target.etl_timestamp = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    sk_security_id, security_id, symbol, issue_type, status, name,
    exchange_id, shares_outstanding, first_trade_date, first_trade_exchange,
    dividend, company_id, is_current, etl_timestamp
) VALUES (
    source.sk_security_id, source.security_id, source.symbol, source.issue_type,
    source.status, source.name, source.exchange_id, source.shares_outstanding,
    source.first_trade_date, source.first_trade_exchange, source.dividend,
    source.company_id, true, current_timestamp()
);
