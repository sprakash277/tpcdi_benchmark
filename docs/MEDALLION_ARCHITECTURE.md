# Medallion Architecture for TPC-DI Benchmark

This document describes the Medallion (Bronze/Silver/Gold) architecture implementation for the TPC-DI benchmark.

## Overview

The Medallion Architecture is a data design pattern that organizes data into layers based on quality and transformation stages:

```
Raw Source Files → Bronze (Raw) → Silver (Cleaned) → Gold (Business)
```

## Architecture Layers

### 1. Bronze Layer (Raw / Ingestion)

**Purpose**: Capture raw source data exactly as-is with minimal transformation.

**Transformations Applied**:
- Add `_load_timestamp` (when data was ingested)
- Add `_source_file` (which file the data came from)
- Add `_batch_id` (which batch the data belongs to)

**No other transformations** - preserves original data fidelity.

#### Bronze Tables

| Table | Source File | Format | Description |
|-------|-------------|--------|-------------|
| `bronze_customer_mgmt` | CustomerMgmt.xml | XML as struct | Raw XML structure with nested Customer/Account elements |
| `bronze_finwire` | FINWIRE* | Text (raw_line) | Fixed-width strings (not parsed) |
| `bronze_trade` | Trade.txt | Text (raw_line) | Pipe-delimited raw lines |
| `bronze_daily_market` | DailyMarket.txt | Text (raw_line) | Pipe-delimited raw lines |
| `bronze_hr` | HR.csv | Text (raw_line) | CSV raw lines |
| `bronze_prospect` | Prospect.csv | Text (raw_line) | CSV raw lines |
| `bronze_date` | Date.txt | Text (raw_line) | Reference data |
| `bronze_time` | Time.txt | Text (raw_line) | Reference data |
| `bronze_status_type` | StatusType.txt | Text (raw_line) | Reference data |
| `bronze_tax_rate` | TaxRate.txt | Text (raw_line) | Reference data |
| `bronze_trade_type` | TradeType.txt | Text (raw_line) | Reference data |
| `bronze_industry` | Industry.txt | Text (raw_line) | Reference data |
| `bronze_cash_transaction` | CashTransaction.txt | Text (raw_line) | Transaction data |
| `bronze_holding_history` | HoldingHistory.txt | Text (raw_line) | Holdings data |
| `bronze_watch_history` | WatchHistory.txt | Text (raw_line) | Watch list data |

### 2. Silver Layer (Cleaned / Refined)

**Purpose**: Parse, clean, and transform raw data into structured, typed tables.

**Transformations Applied**:
- Parse delimited/fixed-width formats into columns
- Type conversions (strings → decimals, timestamps, etc.)
- Extract nested XML fields
- SCD Type 2 versioning support (effective_date, end_date, is_current)
- Data cleansing and standardization

#### Silver Tables

| Table | Source Bronze Table | Key Transformations |
|-------|---------------------|---------------------|
| `silver_customers` | `bronze_customer_mgmt` | Extract Customer fields from XML, add SCD columns |
| `silver_accounts` | `bronze_customer_mgmt` | Extract Account fields from XML, add SCD columns |
| `silver_companies` | `bronze_finwire` | Parse CMP records (positions 16-18 = 'CMP') |
| `silver_securities` | `bronze_finwire` | Parse SEC records (positions 16-18 = 'SEC') |
| `silver_financials` | `bronze_finwire` | Parse FIN records (positions 16-18 = 'FIN') |
| `silver_trades` | `bronze_trade` | Parse 14-column pipe format, type conversions |
| `silver_daily_market` | `bronze_daily_market` | Parse 6-column pipe format, type conversions |
| `silver_date` | `bronze_date` | Parse 18-column date dimension |
| `silver_status_type` | `bronze_status_type` | Parse ST_ID\|ST_NAME |
| `silver_trade_type` | `bronze_trade_type` | Parse TT_ID\|TT_NAME\|TT_IS_SELL\|TT_IS_MRKT |
| `silver_industry` | `bronze_industry` | Parse IN_ID\|IN_NAME\|IN_SC_ID\|IN_SC_NAME |

### 3. Gold Layer (Business / Dimensional)

**Purpose**: Business-ready dimensional model for analytics.

**Note**: The benchmark uses medallion only (Bronze → Silver). The Gold layer (DimXxx, FactXxx) is not implemented in this flow.

## File Format Handling

### 1. Pipe-Delimited Files (.txt)
Most TPC-DI files use pipe (`|`) as delimiter:
- Trade.txt, DailyMarket.txt, Date.txt, StatusType.txt, etc.

**Bronze**: Stored as single `raw_line` column
**Silver**: Parsed using SQL `split(raw_line, '|')` function

### 2. CSV Files (.csv)
HR.csv and Prospect.csv use comma delimiters:

**Bronze**: Stored as single `raw_line` column
**Silver**: Parsed using SQL `split(raw_line, ',')` function

### 3. XML Files (CustomerMgmt.xml)
Complex nested XML with customer and account data:

**Bronze**: Parsed using spark-xml library, stored as nested struct
**Silver**: Extracted specific fields using DataFrame column access (`Customer._C_ID`, `Customer.Account._CA_ID`, etc.)

### 4. Fixed-Width Files (FINWIRE*)
Mixed record types with character position parsing:

**Bronze**: Stored as single `raw_line` column (unparsed)
**Silver**: Parsed using `substring(raw_line, position, length)` based on record type:
- Positions 16-18 = 'CMP' → Company record
- Positions 16-18 = 'SEC' → Security record
- Positions 16-18 = 'FIN' → Financial record

## FINWIRE Record Layouts

### CMP (Company) Record
| Position | Length | Field |
|----------|--------|-------|
| 1-15 | 15 | PTS (Timestamp) |
| 16-18 | 3 | RecType = 'CMP' |
| 19-78 | 60 | CompanyName |
| 79-88 | 10 | CIK |
| 89-92 | 4 | Status |
| 93-102 | 10 | IndustryID |
| 103-106 | 4 | SPRating |
| 107-114 | 8 | FoundingDate |
| 115-129 | 15 | CEOName |
| 130-174 | 45 | AddressLine1 |
| 175-219 | 45 | AddressLine2 |
| 220-244 | 25 | PostalCode |
| 245-269 | 25 | City |
| 270-294 | 25 | StateProvince |
| 295-319 | 25 | Country |
| 320-364 | 45 | Description |

### SEC (Security) Record
| Position | Length | Field |
|----------|--------|-------|
| 1-15 | 15 | PTS |
| 16-18 | 3 | RecType = 'SEC' |
| 19-33 | 15 | Symbol |
| 34-39 | 6 | IssueType |
| 40-49 | 10 | Status |
| 50-119 | 70 | Name |
| 120-131 | 12 | ExID |
| 132-149 | 18 | ShOut |
| 150-165 | 16 | FirstTradeDate |
| 166-181 | 16 | FirstTradeExchg |
| 182-189 | 8 | Dividend |
| 190-249 | 60 | CoNameOrCIK |

### FIN (Financial) Record
| Position | Length | Field |
|----------|--------|-------|
| 1-15 | 15 | PTS |
| 16-18 | 3 | RecType = 'FIN' |
| 19-22 | 4 | Year |
| 23 | 1 | Quarter |
| 24-33 | 10 | QtrStartDate |
| 34-50 | 17 | PostingDate |
| 51-67 | 17 | Revenue |
| 68-84 | 17 | Earnings |
| 85-101 | 17 | EPS |
| 102-118 | 17 | DilutedEPS |
| 119-135 | 17 | Margin |
| 136-152 | 17 | Inventory |
| 153-169 | 17 | Assets |
| 170-186 | 17 | Liabilities |
| 187-203 | 17 | ShOut |
| 204-213 | 10 | DilutedShOut |
| 214-273 | 60 | CoNameOrCIK |

## Usage

The benchmark always runs the medallion pipeline (Bronze → Silver layers). No architecture selection is required.

### Databricks Notebook

Run the benchmark notebook with the usual widgets (load type, scale factor, output path, etc.). Bronze and Silver ETL run automatically.

### CLI

```bash
python run_benchmark_databricks.py \
  --output-path /Volumes/catalog/schema/volume \
  --use-volume \
  --scale-factor 10
```

### Workflow

Use the Databricks workflow with the standard parameters (output_path, use_volume, load_type, etc.).

## SCD Type 2 Support

The Silver layer includes columns for SCD Type 2 tracking:

| Column | Description |
|--------|-------------|
| `is_current` | Whether this is the current version |
| `effective_date` | When this version became active |
| `end_date` | When this version was superseded (NULL for current) |
| `batch_id` | Which batch created/updated this record |

## Incremental Processing

For incremental batches (Batch2, Batch3, etc.):

1. **Bronze**: New data is appended to existing bronze tables with `batch_id` tracking
2. **Silver**: New data is appended with proper SCD handling

The `_batch_id` column in Bronze and `batch_id` in Silver allows you to trace data lineage back to the source batch.

## Best Practices

1. **Always start fresh for Batch1**: The first batch (historical load) uses `mode="overwrite"` to create clean tables.

2. **Incremental batches append**: Batch2+ uses `mode="append"` with schema merge enabled.

3. **Use Unity Catalog**: For production workloads, use Unity Catalog with proper catalog/schema organization.

4. **Monitor Bronze for debugging**: If Silver transformations fail, check the Bronze tables to see the raw data.

5. **Enable spark-xml**: The `CustomerMgmt.xml` parsing requires the spark-xml library (`com.databricks:spark-xml_2.12:0.15.0`).
