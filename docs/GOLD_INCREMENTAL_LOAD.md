# Gold Layer and Incremental Load in TPC-DI Medallion

This document explains the recommendation for Gold tables when doing incremental load in a TPC-DI medallion (Bronze → Silver → Gold) architecture.

## Context

- **Gold** = business-ready star schema (dimensions and facts) that BI/analytics query.
- **Silver** = cleaned, conformed layer; Gold is derived from Silver.
- On **incremental** batches (2+), Silver is updated incrementally (e.g. merge/append, SCD Type 2). Gold then reads from that updated Silver.

## Recommendation for Gold on Incremental Load

Two common patterns:

### 1. Full refresh from Silver (typical TPC-DI approach)

- On each run (batch or incremental), **rebuild Gold entirely** from the current Silver tables (overwrite each Gold table).
- Silver already holds the full history (batch 1 + later batches); Gold is the current snapshot or reporting view of that.
- **Recommendation:** Treat Gold as a **full refresh from Silver** on every run. This is the usual TPC-DI–style approach: simple, consistent, and correct as long as Silver is the source of truth.

### 2. Incremental merge into Gold (optional optimization)

- Alternatively, update Gold **incrementally**: only **merge/upsert** changed dimension keys and new fact rows (e.g. MERGE by key, append-only facts).
- TPC-DI does not require this; it is an implementation choice for efficiency (less data written to Gold, faster runs).

## Summary

| Layer  | Incremental load approach |
|--------|----------------------------|
| Silver | Incremental updates (append/merge by batch); holds full, up-to-date history. |
| Gold   | Either **full refresh from Silver** each run (recommended for correctness and simplicity) or **incremental merge** into Gold for performance, as long as results match full-refresh semantics. |

In TPC-DI medallion terms, the **recommendation for Gold on incremental load** is to treat Gold as a **view of current Silver**—either by **rebuilding Gold from Silver on every run** (full refresh) or by **incrementally merging** into Gold in a way that produces the same result. Full refresh from Silver is the straightforward, recommended approach; incremental Gold is an optional optimization.
