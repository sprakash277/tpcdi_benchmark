# Gold Layer Table Schemas

Schema definitions for all Gold layer tables. Gold contains the business-ready dimensional model (star schema) for analytics.

**Target model (TPC-DI v1.1.0 Clause 3):** DimAccount, DimBroker, DimCompany, DimCustomer, DimDate, DimSecurity, DimTime, DimTrade, FactCashBalances, FactHoldings, FactMarketHistory, FactWatches, Industry, etc. This project uses `gold_dim_*` and `gold_fact_*` naming.

Use `catalog.schema` prefix for Unity Catalog.

---

## Dimension Tables

### gold_dim_customer
**Source**: silver_customers (current versions only).

```sql
CREATE TABLE gold_dim_customer (
  sk_customer_id BIGINT,
  customer_id BIGINT,
  tax_id STRING,
  status STRING,
  last_name STRING,
  first_name STRING,
  middle_name STRING,
  gender STRING,
  tier INT,
  dob DATE,
  address_line1 STRING,
  address_line2 STRING,
  postal_code STRING,
  city STRING,
  state_prov STRING,
  country STRING,
  email1 STRING,
  email2 STRING,
  local_tax_id STRING,
  national_tax_id STRING,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_dim_account
**Source**: silver_accounts (current versions only).

```sql
CREATE TABLE gold_dim_account (
  sk_account_id BIGINT,
  account_id BIGINT,
  broker_id BIGINT,
  customer_id BIGINT,
  account_name STRING,
  tax_status INT,
  status_id STRING,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_dim_company
**Source**: silver_companies.

```sql
CREATE TABLE gold_dim_company (
  sk_company_id BIGINT,
  company_id STRING,
  company_name STRING,
  industry_id STRING,
  sector STRING,
  status STRING,
  address_line1 STRING,
  address_line2 STRING,
  postal_code STRING,
  city STRING,
  state_prov STRING,
  country STRING,
  description STRING,
  founding_date STRING,
  ceo_name STRING,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_dim_security
**Source**: silver_securities.

```sql
CREATE TABLE gold_dim_security (
  sk_security_id STRING,
  security_id STRING,
  symbol STRING,
  issue_type STRING,
  status STRING,
  name STRING,
  exchange_id STRING,
  shares_outstanding BIGINT,
  first_trade_date STRING,
  first_trade_exchange STRING,
  dividend DOUBLE,
  company_id STRING,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_dim_date
**Source**: silver_date.

```sql
CREATE TABLE gold_dim_date (
  sk_date_id INT,
  date_id INT,
  date_value DATE,
  date_desc STRING,
  calendar_year_id INT,
  calendar_year_desc STRING,
  calendar_qtr_id INT,
  calendar_qtr_desc STRING,
  calendar_month_id INT,
  calendar_month_desc STRING,
  calendar_week_id INT,
  calendar_week_desc STRING,
  day_of_week_num INT,
  day_of_week_desc STRING,
  fiscal_year_id INT,
  fiscal_year_desc STRING,
  fiscal_qtr_id INT,
  fiscal_qtr_desc STRING,
  holiday_flag BOOLEAN,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_dim_trade_type
**Source**: silver_trade_type.

```sql
CREATE TABLE gold_dim_trade_type (
  sk_trade_type_id STRING,
  trade_type_id STRING,
  trade_type_code STRING,
  trade_type_name STRING,
  is_sell BOOLEAN,
  is_market BOOLEAN,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_dim_status_type
**Source**: silver_status_type.

```sql
CREATE TABLE gold_dim_status_type (
  sk_status_type_id STRING,
  status_type_id STRING,
  status_type_code STRING,
  status_type_name STRING,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_dim_industry
**Source**: silver_industry.

```sql
CREATE TABLE gold_dim_industry (
  sk_industry_id STRING,
  industry_id STRING,
  industry_name STRING,
  sector_id STRING,
  sector_name STRING,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

## Fact Tables

### gold_fact_trade
**Source**: silver_trades (current versions) joined with gold_dim_date, gold_dim_account, gold_dim_customer, gold_dim_security, gold_dim_trade_type.

```sql
CREATE TABLE gold_fact_trade (
  sk_date_id INT,
  sk_customer_id BIGINT,
  sk_account_id BIGINT,
  sk_security_id STRING,
  sk_trade_type_id STRING,
  trade_id BIGINT,
  trade_dts TIMESTAMP,
  trade_price DOUBLE,
  trade_quantity INT,
  trade_amount DOUBLE,
  commission DOUBLE,
  charge DOUBLE,
  tax DOUBLE,
  status_id STRING,
  is_cash BOOLEAN,
  exec_name STRING,
  batch_id INT,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_fact_market_history
**Source**: silver_daily_market (current versions) joined with gold_dim_date, gold_dim_security.

```sql
CREATE TABLE gold_fact_market_history (
  sk_date_id INT,
  sk_security_id STRING,
  market_date DATE,
  symbol STRING,
  close_price DOUBLE,
  high_price DOUBLE,
  low_price DOUBLE,
  volume BIGINT,
  batch_id INT,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_fact_holdings
**Source**: silver_holding_history (current versions) joined with gold_dim_date, gold_dim_account, gold_dim_security.

```sql
CREATE TABLE gold_fact_holdings (
  sk_date_id INT,
  sk_account_id BIGINT,
  sk_security_id STRING,
  account_id BIGINT,
  symbol STRING,
  quantity BIGINT,
  purchase_price DOUBLE,
  purchase_date DATE,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```

---

### gold_fact_cash_balances
**Source**: silver_cash_transaction aggregated by account and date. *Note: Requires account_id in silver_cash_transaction (via join with trades); schema may vary.*

```sql
CREATE TABLE gold_fact_cash_balances (
  sk_date_id INT,
  sk_account_id BIGINT,
  account_id BIGINT,
  cash_balance DOUBLE,
  transaction_count BIGINT,
  etl_timestamp TIMESTAMP
)
USING DELTA;
```
