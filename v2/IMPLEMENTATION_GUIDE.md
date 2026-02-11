# TPC-DI v2 Implementation Guide

## Overview

This v2 implementation provides SQL-only implementations of the TPC-DI specification following the Medallion Architecture (Bronze → Silver → Gold). The codebase is organized into platform-specific folders for Databricks and Dataproc.

## Directory Structure

```
v2/
├── databricks/          # Databricks-specific SQL (Delta Lake, Unity Catalog)
│   ├── bronze/          # Bronze layer DDL and DML
│   │   ├── 01_create_bronze_tables.sql
│   │   ├── 02_load_bronze_batch1.sql
│   │   └── 03_load_bronze_incremental.sql
│   ├── silver/          # Silver layer DDL and DML
│   │   ├── 01_create_silver_tables.sql
│   │   ├── 02_transform_silver_batch1.sql
│   │   └── 03_transform_silver_incremental.sql
│   └── gold/            # Gold layer DDL and DML
│       ├── 01_create_gold_tables.sql
│       ├── 02_load_gold_batch1.sql
│       └── 03_load_gold_incremental.sql
├── dataproc/            # Dataproc-specific SQL (Delta Lake on GCS)
│   ├── bronze/          # Same structure as Databricks
│   ├── silver/
│   └── gold/
├── README.md            # Overview
└── IMPLEMENTATION_GUIDE.md  # This file
```

## Key Differences: Databricks vs Dataproc

### Databricks
- **Storage**: DBFS/Volumes (Unity Catalog)
- **Catalog**: Unity Catalog (`USE CATALOG ...`)
- **Schema**: Unity Catalog schemas (`USE SCHEMA ...`)
- **File Reading**: Native `read_files()` function
- **XML Support**: Native XML reader or spark-xml

### Dataproc
- **Storage**: GCS (`gs://bucket/path`)
- **Catalog**: Hive Metastore (`USE DATABASE ...`)
- **Schema**: Database schemas
- **File Reading**: `read_files()` with GCS paths
- **XML Support**: Requires spark-xml JAR (`com.databricks:spark-xml_2.12:0.18.0`)
- **Table Location**: Explicit `LOCATION 'gs://...'` clauses

## Execution Order

### Batch 1 (Historical Load)

1. **Bronze Layer**
   - Execute `01_create_bronze_tables.sql` (create tables)
   - Execute `02_load_bronze_batch1.sql` (load raw data)

2. **Silver Layer**
   - Execute `01_create_silver_tables.sql` (create tables)
   - Execute `02_transform_silver_batch1.sql` (transform Bronze → Silver)

3. **Gold Layer**
   - Execute `01_create_gold_tables.sql` (create tables)
   - Execute `02_load_gold_batch1.sql` (load Silver → Gold)

### Batch 2+ (Incremental Load)

1. **Bronze Layer**
   - Execute `03_load_bronze_incremental.sql` (append raw data)

2. **Silver Layer**
   - Execute `03_transform_silver_incremental.sql` (MERGE Bronze → Silver)

3. **Gold Layer**
   - Execute `03_load_gold_incremental.sql` (MERGE Silver → Gold)

## Configuration Variables

Before executing SQL files, set these variables:

### Databricks
```sql
SET var.raw_data_path = '/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10';
SET var.batch_id = 1;  -- Change for incremental loads
USE CATALOG tpcdi_catalog;
USE SCHEMA bronze_schema;  -- Change for silver/gold
```

### Dataproc
```sql
SET var.raw_data_path = 'gs://YOUR_BUCKET/tpcdi/sf=10';
SET var.batch_id = 1;  -- Change for incremental loads
USE DATABASE tpcdi_bronze;  -- Change for silver/gold
```

## Schema Patterns

### Bronze Layer
- **Purpose**: Raw ingestion, no transformations
- **Columns**: All STRING type (raw data)
- **Metadata**: `_batch_id`, `_load_timestamp`, `_source_file`
- **Source Formats**: XML, Fixed-Width, Pipe-delimited, CSV

### Silver Layer
- **Purpose**: Cleaned, typed, conformed data
- **SCD Type 2**: `is_current`, `effective_date`, `end_date` for dimensions
- **Business Keys**: Used for MERGE operations
- **Data Quality**: Type casting, validation

### Gold Layer
- **Purpose**: Business-ready star schema
- **Surrogate Keys**: `SK_*` columns for stability
- **Dimensions**: Current versions only (SCD Type 1 in Gold)
- **Facts**: Append-only (except FactHoldings which updates state)

## Key Tables

### Bronze Tables (14 source file types)
- `bronze_customer_mgmt` (XML, Batch 1)
- `bronze_customer` (Pipe-delimited, Batch 2+)
- `bronze_account` (Pipe-delimited, Batch 2+)
- `bronze_finwire` (Fixed-width, Batch 1)
- `bronze_trade`, `bronze_daily_market`, `bronze_cash_transaction`
- `bronze_holding_history`, `bronze_watch_history`
- `bronze_date`, `bronze_time`, `bronze_status_type`
- `bronze_trade_type`, `bronze_industry`, `bronze_tax_rate`
- `bronze_hr` (CSV, Batch 1)
- `bronze_prospect` (CSV, all batches)

### Silver Tables
- **Dimensions**: `silver_customers`, `silver_accounts` (SCD Type 2)
- **Market Data**: `silver_companies`, `silver_securities`, `silver_financials`
- **Transactions**: `silver_trades`, `silver_daily_market`, `silver_cash_transaction`
- **History**: `silver_holding_history`, `silver_watch_history`
- **Reference**: `silver_date`, `silver_time`, `silver_status_type`, etc.

### Gold Tables
- **Dimensions**: `gold_dim_customer`, `gold_dim_account`, `gold_dim_company`, `gold_dim_security`, `gold_dim_date`, `gold_dim_time`, `gold_dim_broker`, `gold_dim_trade_type`, `gold_dim_status_type`, `gold_dim_industry`
- **Facts**: `gold_fact_trade`, `gold_fact_market_history`, `gold_fact_cash_balances`, `gold_fact_holdings`, `gold_fact_watches`
- **Other**: `gold_financials`, `gold_prospect`
- **Audit**: `gold_dim_messages` (TPC-DI spec requirement)

## SCD Type 2 Implementation

### Silver Layer
- **Full History**: All changes tracked with `is_current`, `effective_date`, `end_date`
- **MERGE Logic**: Close old records (`is_current = false`, set `end_date`), insert new versions

### Gold Layer
- **Current Only**: Dimensions store only current versions (`is_current = true`)
- **Incremental**: Uses MERGE upsert (SCD Type 1) to update latest state

## Late-Arriving Data

The implementation handles late-arriving dimensions/facts:
- **Placeholder Keys**: Use `-1` for missing customer/account IDs
- **DI_Messages**: Logs alerts when facts reference missing dimensions
- **Reconciliation**: Can backfill when dimensions arrive later

## Data Quality

### DI_Messages Table
Per TPC-DI spec, all data quality issues are logged:
- **Referential Integrity**: Missing foreign keys
- **Data Validation**: Invalid values, type mismatches
- **Business Rules**: Violations of domain constraints

## Performance Considerations

### Delta Lake Optimizations
- **Auto-Optimize**: Enabled via `delta.autoOptimize.optimizeWrite` and `delta.autoOptimize.autoCompact`
- **Z-Ordering**: Consider for high-cardinality dimensions (not implemented in v2)
- **Partitioning**: Not implemented in v2 (can be added per table)

### Batch vs Incremental
- **Batch 1**: Full table overwrite (`INSERT OVERWRITE`)
- **Batch 2+**: MERGE operations for efficiency

## Testing

1. **Batch 1**: Run full historical load
2. **Batch 2**: Run incremental load, verify MERGE logic
3. **Data Quality**: Check `gold_dim_messages` for alerts
4. **Referential Integrity**: Verify all foreign keys resolve

## Next Steps

1. **Customize Paths**: Update `YOUR_BUCKET` and paths in Dataproc files
2. **Add Partitioning**: Consider partitioning large fact tables by date
3. **Add Z-Ordering**: Optimize query performance for common filters
4. **Add Monitoring**: Set up alerts on `gold_dim_messages`
5. **Performance Tuning**: Adjust Delta Lake properties based on workload
