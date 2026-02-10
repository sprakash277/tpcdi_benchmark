# Gold Layer and Incremental Load in TPC-DI Medallion

This document explains the recommendation for Gold tables when doing incremental load in a TPC-DI medallion (Bronze → Silver → Gold) architecture, and how this benchmark implements spec-aligned Gold behavior.

## Context

- **Gold** = business-ready star schema (dimensions and facts) that BI/analytics query.
- **Silver** = cleaned, conformed layer; Gold is derived from Silver.
- On **incremental** batches (2+), Silver is updated incrementally (e.g. merge/append, SCD Type 2). Gold then reads from that updated Silver.

## TPC-DI Spec-Aligned Gold (This Benchmark)

This benchmark implements Gold tables per the TPC-DI spec:

| Table type   | Spec requirement | Implementation (Databricks & Dataproc) |
|-------------|-------------------|----------------------------------------|
| **Dimensions** | SCD Type 2 in Gold | **MERGE** with logic to **expire old rows** (set `is_current=false`, `end_date=effective_date`) and **insert new** rows. Applied to **DimCustomer** and **DimAccount** (Delta MERGE). Reference dimensions (Date, TradeType, StatusType, Industry, Company, Security) remain overwrite. |
| **Fact tables** | Append only after $SK$ lookups | On **incremental** runs, fact tables use **INSERT (append)** only; $SK$ lookups are performed by joining Silver facts to Gold dimensions before write. On **batch** runs, fact tables are overwrite (initial load). |
| **Financials**  | SCD Type 1 | **MERGE (upsert)** so the latest financial figures are reflected. **Gold financials** is loaded from `silver_financials` with merge key `(co_name_or_cik, year, quarter)`. |

### Implementation details

- **Dimensions (SCD Type 2):** Gold DimCustomer and DimAccount include `effective_date`, `end_date`, `is_current`. Platform method `merge_scd2()` runs Delta MERGE: match on business key + `is_current=true`, then UPDATE set `is_current=false`, `end_date=source.effective_date`; WHEN NOT MATCHED INSERT new row. Fact loaders join to dimensions filtered by `is_current=true` for $SK$ resolution.
- **Facts (append only):** Runner passes `load_type` and `batch_id` to Gold ETL. When `load_type == INCREMENTAL`, all Gold fact loaders use `mode="append"`; otherwise `mode="overwrite"`.
- **Financials (SCD Type 1):** Platform method `merge_upsert()` runs Delta MERGE on key columns; when table does not exist, it is created with overwrite. Gold financials loader reads `silver_financials` and calls `merge_upsert` with keys `["co_name_or_cik", "year", "quarter"]`.

## Recommendation for Gold on Incremental Load (General)

Two common patterns:

### 1. Full refresh from Silver (simple approach)

- On each run (batch or incremental), **rebuild Gold entirely** from the current Silver tables (overwrite each Gold table).
- Silver already holds the full history; Gold is the current snapshot or reporting view.
- Correct and simple; no MERGE logic in Gold.

### 2. Spec-aligned incremental Gold (this benchmark)

- **Dimensions:** SCD Type 2 in Gold via MERGE (expire old, insert new).
- **Facts:** Append only after $SK$ lookups on incremental runs.
- **Financials:** SCD Type 1 MERGE (upsert) for latest figures.

## Summary

| Layer  | Incremental load approach in this benchmark |
|--------|--------------------------------------------|
| Silver | Incremental updates (append/merge by batch); holds full, up-to-date history. |
| Gold dimensions (Customer, Account) | SCD Type 2: MERGE to expire old rows and insert new (Delta). |
| Gold dimensions (reference) | Overwrite (Date, TradeType, StatusType, Industry, Company, Security). |
| Gold fact tables | **Append only** on incremental runs (after $SK$ lookups); overwrite on batch. |
| Gold financials | SCD Type 1: MERGE (upsert) on `(co_name_or_cik, year, quarter)`. |

In TPC-DI medallion terms, this benchmark uses **spec-aligned Gold** on incremental load: dimensions (Customer, Account) use SCD Type 2 MERGE in Gold, fact tables are append-only after $SK$ lookups, and financials use SCD Type 1 MERGE. Both **Databricks** and **Dataproc** use the same logic; Delta format is required for MERGE (Databricks default; Dataproc with `--format delta`).
