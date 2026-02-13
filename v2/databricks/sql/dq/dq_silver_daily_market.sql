-- TPC-DI v2: DQ rules for silver_daily_market (from v1 silver_rules.py)
-- Placeholders: __CATALOG__, __SCHEMA__, __BATCH_ID__

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_daily_market', CONCAT('dm_date NULL: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_DailyMarket_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_daily_market WHERE batch_id = __BATCH_ID__ AND dm_date IS NULL) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_daily_market', CONCAT('dm_close < 0: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_DailyMarket_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_daily_market WHERE batch_id = __BATCH_ID__ AND dm_close IS NOT NULL AND dm_close < 0) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_daily_market', CONCAT('dm_high < 0: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_DailyMarket_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_daily_market WHERE batch_id = __BATCH_ID__ AND dm_high IS NOT NULL AND dm_high < 0) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_daily_market', CONCAT('dm_low < 0: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_DailyMarket_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_daily_market WHERE batch_id = __BATCH_ID__ AND dm_low IS NOT NULL AND dm_low < 0) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_daily_market', 'dm_vol < 0', 'Validation', 'Silver_DailyMarket_Validation', 'Alert'
FROM (SELECT 1 FROM __CATALOG__.__SCHEMA__.silver_daily_market WHERE batch_id = __BATCH_ID__ AND dm_vol IS NOT NULL AND dm_vol < 0 LIMIT 1) t;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_daily_market', CONCAT('dm_high < dm_low: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_DailyMarket_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_daily_market WHERE batch_id = __BATCH_ID__ AND dm_high IS NOT NULL AND dm_low IS NOT NULL AND dm_high < dm_low) t WHERE cnt > 0;

INSERT INTO __CATALOG__.__SCHEMA__.gold_dim_messages
SELECT current_timestamp(), __BATCH_ID__, 'silver_daily_market', CONCAT('dm_close outside [dm_low,dm_high]: ', CAST(cnt AS STRING), ' row(s)'), 'Validation', 'Silver_DailyMarket_Validation', 'Alert'
FROM (SELECT COUNT(*) AS cnt FROM __CATALOG__.__SCHEMA__.silver_daily_market WHERE batch_id = __BATCH_ID__ AND dm_close IS NOT NULL AND dm_high IS NOT NULL AND dm_low IS NOT NULL AND (dm_close < dm_low OR dm_close > dm_high)) t WHERE cnt > 0;
