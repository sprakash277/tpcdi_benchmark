# TPC-DI Benchmark: Databricks vs Dataproc (SF 10000)

Same workload: **batch**, **scale factor 10,000**, **12 × n2d-standard-16 workers**, **n2d-standard-16 driver**.

---

## Summary

| Metric | Databricks | Dataproc | Delta |
|--------|------------|----------|--------|
| **Total duration** | 1,259.95 s (~21 min) | 2,217.77 s (~37 min) | Dataproc **+76%** slower |
| **Throughput (rows/sec)** | 19,636,004 | 11,118,888 | Databricks **1.77×** higher |
| **Data throughput (MB/sec)** | 782.90 | 443.32 | Databricks **1.77×** higher |
| **Total rows processed** | 24,653,847,245 | 24,653,847,245 | Same |
| **Total data size (pipeline)** | 982,961.56 MB | 982,961.56 MB | Same |
| **Table-level total data size** | 883,603 MB | 1,357,027 MB | Dataproc ~1.5× larger (storage format) |

---

## Cost (estimated)

| | Databricks | Dataproc |
|--|------------|----------|
| Compute | $2.27 | $6.28 |
| Software | $2.73 | $1.28 |
| **Total** | **$5.00** | **$7.56** |

Dataproc: higher compute cost, lower software; **~51% higher total cost** for this run.

---

## Step timing

| Step | Databricks | Dataproc | Delta |
|------|------------|----------|--------|
| spark_session_creation | 0.09 s | 5.86 s | Dataproc +5.8 s |
| platform_adapter_creation | 0.00 s | 0.00 s | — |
| database_creation | 3.42 s | 2.36 s | — |
| **bronze_etl** | **541.47 s** | **508.78 s** | Dataproc **~33 s faster** |
| **silver_etl** | **420.57 s** | **1,025.52 s** | Dataproc **~2.4× slower** |
| **gold_etl** | **289.98 s** | **674.77 s** | Dataproc **~2.3× slower** |

Silver and gold dominate the gap; bronze is similar or slightly faster on Dataproc.

---

## DQ time (15 tables)

| | Databricks | Dataproc | Delta |
|--|------------|----------|--------|
| **Total DQ** | **88.99 s** | **245.41 s** | Dataproc **~2.8×** longer |

Largest DQ deltas (Dataproc − Databricks):

| Table | Databricks | Dataproc | Delta |
|-------|------------|----------|--------|
| silver_holding_history | 21.82 s | 87.72 s | +65.9 s |
| silver_trades | 14.93 s | 51.08 s | +36.2 s |
| silver_customers | 18.52 s | 26.12 s | +7.6 s |
| silver_daily_market | 9.37 s | 25.07 s | +15.7 s |
| silver_accounts | 5.72 s | 13.58 s | +7.9 s |
| silver_watch_history | 3.39 s | 12.60 s | +9.2 s |
| silver_cash_transaction | 3.03 s | 7.32 s | +4.3 s |
| silver_financials | 2.52 s | 7.24 s | +4.7 s |
| silver_prospect | 3.13 s | 5.33 s | +2.2 s |

---

## Per-table highlights (duration)

| Table | Databricks | Dataproc | Notes |
|-------|------------|----------|--------|
| bronze_etl (aggregate) | 541 s | 509 s | Similar |
| silver_trades | 40.34 s | 100.80 s | Dataproc ~2.5× slower |
| silver_daily_market | 68.35 s | 193.60 s | Dataproc ~2.8× slower |
| silver_cash_transaction | 39.25 s | 71.66 s | Dataproc ~1.8× slower |
| silver_watch_history | 33.72 s | 106.66 s | Dataproc ~3.2× slower |
| silver_holding_history | 42.61 s | 103.18 s | Dataproc ~2.4× slower |
| silver_financials | 26.88 s | 78.11 s | Dataproc ~2.9× slower |
| silver_companies | 12.39 s | 37.87 s | Dataproc ~3× slower |
| silver_securities | 11.79 s | 24.43 s | Dataproc ~2× slower |
| gold_dim_broker | 27.39 s | 80.58 s | Dataproc ~2.9× slower |
| gold_fact_trade | 58.65 s | 123.00 s | Dataproc ~2.1× slower |
| gold_fact_market_history | 55.23 s | 168.15 s | Dataproc ~3× slower |
| gold_fact_cash_balances | 31.73 s | 67.54 s | Dataproc ~2.1× slower |
| gold_fact_holdings | 24.61 s | 67.55 s | Dataproc ~2.7× slower |
| gold_fact_watches | 31.01 s | 92.65 s | Dataproc ~3× slower |

---

## Takeaways

1. **End-to-end:** Databricks **~21 min** vs Dataproc **~37 min** (~76% longer on Dataproc).
2. **Silver + Gold:** Most of the gap is in silver and gold ETL (and DQ); bronze is comparable.
3. **Big tables:** silver_daily_market, silver_trades, silver_watch_history, silver_holding_history, silver_cash_transaction, and the gold fact tables are much slower on Dataproc (often 2–3×).
4. **Cost:** Databricks **$5.00** vs Dataproc **$7.56** for this run (Dataproc higher total cost).
5. **Storage:** Table-level reported data size is larger on Dataproc (1.36M MB vs 884K MB), consistent with different Delta/storage layout or compression.
