# Gold Layer Implementation

## Overview

The Gold layer transforms Silver cleaned data into business-ready, query-optimized star schema tables. Gold tables are denormalized, pre-joined, and contain only current versions (no SCD Type 2 complexity).

## Architecture

```
Silver (Cleaned, Versioned) → Gold (Business-Ready, Star Schema)
```

### Key Differences

| Aspect | Silver | Gold |
|--------|--------|------|
| **Purpose** | Cleaned, validated, versioned | Business-ready, query-optimized |
| **SCD Type 2** | Yes (historical versions) | No (current version only) |
| **Normalization** | Normalized (separate tables) | Denormalized (star schema) |
| **Joins** | Many joins needed | Pre-joined, ready for queries |
| **Updates** | Incremental (SCD2) | Incremental (refresh current) |
| **Query Performance** | Slower (joins + filters) | Faster (pre-joined) |

## Gold Tables

### Dimension Tables

1. **gold_dim_customer** - Current customer versions
   - Source: `silver_customers` (filter: `is_current = true`)
   - Columns: sk_customer_id, customer_id, tax_id, status, name fields, address, emails, tax IDs

2. **gold_dim_account** - Current account versions
   - Source: `silver_accounts` (filter: `is_current = true`)
   - Columns: sk_account_id, account_id, broker_id, customer_id, account_name, tax_status, status_id

3. **gold_dim_company** - Company information
   - Source: `silver_companies`
   - Columns: sk_company_id, company_id (CIK), company_name, industry_id, address, description, CEO, founding_date

4. **gold_dim_security** - Security/stock information
   - Source: `silver_securities`
   - Columns: sk_security_id, security_id (symbol), symbol, issue_type, status, name, exchange, shares_outstanding, dividend

5. **gold_dim_date** - Date dimension
   - Source: `silver_date`
   - Columns: sk_date_id, date_value, calendar/fiscal hierarchies, holiday_flag

6. **gold_dim_trade_type** - Trade type lookup
   - Source: `silver_trade_type`
   - Columns: sk_trade_type_id, trade_type_id (tt_id), trade_type_name, is_sell, is_market

7. **gold_dim_status_type** - Status type lookup
   - Source: `silver_status_type`
   - Columns: sk_status_type_id, status_type_id (st_id), status_type_name

8. **gold_dim_industry** - Industry lookup
   - Source: `silver_industry`
   - Columns: sk_industry_id, industry_id (in_id), industry_name, sector_id, sector_name

### Fact Tables

1. **gold_fact_trade** - Trade transactions (denormalized)
   - Source: `silver_trades` + dimension joins
   - Joins:
     - `silver_trades.account_id` → `gold_dim_account.account_id` → `gold_dim_customer.customer_id`
     - `silver_trades.symbol` → `gold_dim_security.symbol`
     - `to_date(silver_trades.trade_dts)` → `gold_dim_date.date_value`
     - `silver_trades.trade_type_id` → `gold_dim_trade_type.trade_type_id`
   - Columns: sk_date_id, sk_customer_id, sk_account_id, sk_security_id, sk_trade_type_id, trade_id, trade_dts, trade_price, trade_quantity, trade_amount, commission, charge, tax, status_id, is_cash, exec_name

2. **gold_fact_market_history** - Daily market data (denormalized)
   - Source: `silver_daily_market` + dimension joins
   - Joins:
     - `silver_daily_market.dm_date` → `gold_dim_date.date_value`
     - `silver_daily_market.dm_s_symb` → `gold_dim_security.symbol`
   - Columns: sk_date_id, sk_security_id, market_date, symbol, close_price, high_price, low_price, volume

3. **gold_fact_cash_balances** - Cash transaction aggregates (optional)
   - Source: `silver_cash_transaction` (when implemented)
   - Aggregates cash transactions by account and date

4. **gold_fact_holdings** - Current holdings (optional)
   - Source: `silver_holding_history` (when implemented)
   - Current holdings with dimension keys

## Usage

### Batch Load

Gold layer runs automatically after Silver layer:

```python
from benchmark.runner import run_benchmark
from benchmark.config import BenchmarkConfig, Platform, LoadType

config = BenchmarkConfig(
    platform=Platform.DATABRICKS,
    load_type=LoadType.BATCH,
    scale_factor=10,
    # ... other config
)

result = run_benchmark(config)
# Gold tables are created automatically
```

### Incremental Load

Gold layer refreshes after Silver incremental updates:

```python
config = BenchmarkConfig(
    platform=Platform.DATABRICKS,
    load_type=LoadType.INCREMENTAL,
    batch_id=2,
    # ... other config
)

result = run_benchmark(config)
# Gold tables are refreshed with current versions
```

### Manual Gold Load

```python
from benchmark.etl.gold import GoldETL
from benchmark.platforms.databricks import DatabricksPlatform

platform = DatabricksPlatform(...)
gold_etl = GoldETL(platform)
gold_etl.run_gold_load("tpcdi_warehouse", "dw")
```

## Implementation Details

### Current Version Selection

Dimensions with SCD Type 2 (customers, accounts) filter for `is_current = true`:

```python
current_df = silver_df.filter(col("is_current") == True)
```

Fact tables (trades, daily_market) use all records (no SCD2).

### Dimension Joins

Fact tables join Silver facts with Gold dimensions to get surrogate keys:

```python
fact_df = silver_trades \
    .join(dim_date, to_date(col("trade_dts")) == dim_date["date_value"], "left") \
    .join(dim_account, col("account_id") == dim_account["account_id"], "left") \
    .join(dim_customer, dim_account["customer_id"] == dim_customer["customer_id"], "left") \
    .join(dim_security, col("symbol") == dim_security["symbol"], "left") \
    .join(dim_trade_type, col("trade_type_id") == dim_trade_type["trade_type_id"], "left")
```

### Write Mode

- **Batch Load**: `mode="overwrite"` (full refresh)
- **Incremental Load**: `mode="overwrite"` (refresh all Gold tables from current Silver)

## Benefits

1. **Query Performance**: Pre-joined tables eliminate runtime joins
2. **Business Semantics**: Aligned with business terms and metrics
3. **BI Tool Ready**: Works directly with Power BI, Tableau, etc.
4. **Current State**: Only current versions (no SCD2 complexity)
5. **Analytics Optimized**: Denormalized structure for fast aggregations

## File Structure

```
benchmark/etl/gold/
├── __init__.py          # GoldETL orchestrator
├── base.py              # GoldLoaderBase
├── dimensions.py        # Dimension loaders (DimCustomer, DimAccount, etc.)
└── facts.py             # Fact loaders (FactTrade, FactMarketHistory, etc.)
```
