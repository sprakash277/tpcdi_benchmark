# TPC-DI v2: SQL-Only Implementation

This folder contains SQL-only implementations of the TPC-DI specification following the Medallion Architecture (Bronze → Silver → Gold).

## Structure

```
v2/
├── databricks/          # Databricks-specific SQL (Delta Lake, Unity Catalog)
│   ├── bronze/          # Bronze layer DDL and DML
│   ├── silver/          # Silver layer DDL and DML
│   └── gold/            # Gold layer DDL and DML
├── dataproc/            # Dataproc-specific SQL (Delta Lake on GCS)
│   ├── bronze/          # Bronze layer DDL and DML
│   ├── silver/          # Silver layer DDL and DML
│   └── gold/            # Gold layer DDL and DML
└── README.md            # This file
```

## Key Differences from v1

- **SQL-only**: Pure SQL/DDL files, no Python ETL code
- **Platform-specific**: Separate implementations for Databricks and Dataproc
- **Spec-aligned**: Follows TPC-DI v1.1.0 specification exactly
- **Medallion Architecture**: Clear separation of Bronze (raw), Silver (cleaned), Gold (business-ready)

## Usage

### Databricks
Execute SQL files in order:
1. `databricks/bronze/*.sql` - Create Bronze tables
2. `databricks/silver/*.sql` - Create Silver tables and transformations
3. `databricks/gold/*.sql` - Create Gold star schema tables

### Dataproc
Execute SQL files in order:
1. `dataproc/bronze/*.sql` - Create Bronze tables
2. `dataproc/silver/*.sql` - Create Silver tables and transformations
3. `dataproc/gold/*.sql` - Create Gold star schema tables

## Batch vs Incremental

- **Batch 1 (Historical)**: Use `INSERT` statements
- **Batch 2+ (Incremental)**: Use `MERGE` statements for SCD Type 2 dimensions

## Schema Patterns

### Bronze Layer
- All columns as STRING (raw ingestion)
- Metadata: `_batch_id`, `_load_timestamp`, `_source_file`

### Silver Layer
- Typed columns (parsed from Bronze)
- SCD Type 2: `is_current`, `effective_date`, `end_date`
- Business keys for MERGE operations

### Gold Layer
- Surrogate Keys (SK_*)
- Star schema (dimensions + facts)
- SCD Type 2 for dimensions (DimCustomer, DimAccount, DimSecurity)
- Append-only facts (FactTrade, FactMarketHistory)
