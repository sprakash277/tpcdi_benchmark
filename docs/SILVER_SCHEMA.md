# Silver Layer Table Schemas

Schema definitions for all Silver layer tables. Silver contains parsed, cleaned, and typed data. Tables with SCD Type 2 include `is_current`, `effective_date`, `end_date`, and `record_type`.

**Source formats (TPC-DI v1.1.0 Clause 2):** Pipe-delimited files are parsed with `split(raw_line, '|')`; CSV with comma. Column names and counts follow the spec (see `docs/TPCDI_SPEC_ALIGNMENT.md`).

Use `catalog.schema` prefix for Unity Catalog.

---

## silver_customers
**Source**: bronze_customer_mgmt (Batch 1), bronze_customer (Batch 2+). SCD Type 2 on `customer_id`.

```sql
CREATE TABLE silver_customers (
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
  batch_id INT,
  is_current BOOLEAN,
  effective_date TIMESTAMP,
  end_date TIMESTAMP,
  load_timestamp TIMESTAMP,
  record_type STRING
)
USING DELTA;
```

---

## silver_accounts
**Source**: bronze_customer_mgmt (Batch 1), bronze_account (Batch 2+). SCD Type 2 on `account_id`.

```sql
CREATE TABLE silver_accounts (
  account_id BIGINT,
  broker_id BIGINT,
  customer_id BIGINT,
  account_name STRING,
  tax_status INT,
  status_id STRING,
  action_type STRING,
  action_timestamp TIMESTAMP,
  effective_date DATE,
  end_date DATE,
  is_current BOOLEAN,
  batch_id INT,
  load_timestamp TIMESTAMP,
  record_type STRING
)
USING DELTA;
```

---

## silver_trades
**Source**: bronze_trade. SCD Type 2 on `trade_id`.

```sql
CREATE TABLE silver_trades (
  record_type STRING,
  trade_id BIGINT,
  trade_dts TIMESTAMP,
  status_id STRING,
  trade_type_id STRING,
  is_cash BOOLEAN,
  symbol STRING,
  quantity INT,
  bid_price DOUBLE,
  account_id BIGINT,
  exec_name STRING,
  trade_price DOUBLE,
  charge DOUBLE,
  commission DOUBLE,
  tax DOUBLE,
  batch_id INT,
  load_timestamp TIMESTAMP,
  effective_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN
)
USING DELTA;
```

---

## silver_daily_market
**Source**: bronze_daily_market. SCD Type 2 on `dm_key`.

```sql
CREATE TABLE silver_daily_market (
  record_type STRING,
  dm_date DATE,
  dm_s_symb STRING,
  dm_close DOUBLE,
  dm_high DOUBLE,
  dm_low DOUBLE,
  dm_vol BIGINT,
  batch_id INT,
  load_timestamp TIMESTAMP,
  dm_key STRING,
  effective_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN
)
USING DELTA;
```

---

## silver_watch_history
**Source**: bronze_watch_history. SCD Type 2 on `wh_key`. Spec 2.2.18: W_C_ID, W_S_SYMB, W_DTS, W_ACTION (pipe).

```sql
CREATE TABLE silver_watch_history (
  record_type STRING,
  w_c_id BIGINT,
  w_s_symb STRING,
  w_dts STRING,
  w_action STRING,
  batch_id INT,
  load_timestamp TIMESTAMP,
  wh_key STRING,
  effective_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN
)
USING DELTA;
```

---

## silver_cash_transaction
**Source**: bronze_cash_transaction. SCD Type 2 on `ct_key`. Spec 2.2.5: CT_CA_ID, CT_DTS, CT_AMT, CT_NAME (pipe).

```sql
CREATE TABLE silver_cash_transaction (
  record_type STRING,
  ct_ca_id BIGINT,
  ct_dts STRING,
  ct_amt DOUBLE,
  ct_name STRING,
  account_id BIGINT,
  batch_id INT,
  load_timestamp TIMESTAMP,
  transaction_date TIMESTAMP,
  ct_key STRING,
  effective_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN
)
USING DELTA;
```

---

## silver_holding_history
**Source**: bronze_holding_history, joined with silver_trades. SCD Type 2 on `hh_h_t_id`.

```sql
CREATE TABLE silver_holding_history (
  record_type STRING,
  hh_h_t_id BIGINT,
  hh_t_id BIGINT,
  hh_before_qty BIGINT,
  hh_after_qty BIGINT,
  account_id BIGINT,
  symbol STRING,
  holding_date TIMESTAMP,
  quantity BIGINT,
  purchase_price DOUBLE,
  purchase_date DATE,
  batch_id INT,
  load_timestamp TIMESTAMP,
  effective_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN
)
USING DELTA;
```

---

## silver_prospect
**Source**: bronze_prospect. Append for incremental.

```sql
CREATE TABLE silver_prospect (
  agency_id STRING,
  last_name STRING,
  first_name STRING,
  middle_initial STRING,
  gender STRING,
  address_line1 STRING,
  address_line2 STRING,
  city STRING,
  state STRING,
  country STRING,
  phone STRING,
  income STRING,
  number_cars STRING,
  number_children STRING,
  marital_status STRING,
  age STRING,
  credit_rating STRING,
  own_or_rent_flag STRING,
  number_credit_cards STRING,
  company_name STRING,
  batch_id INT,
  load_timestamp TIMESTAMP
)
USING DELTA;
```

---

## silver_companies
**Source**: bronze_finwire (CMP records).

```sql
CREATE TABLE silver_companies (
  pts STRING,
  company_name STRING,
  cik STRING,
  status STRING,
  industry_id STRING,
  sp_rating STRING,
  founding_date STRING,
  ceo_name STRING,
  address_line1 STRING,
  address_line2 STRING,
  postal_code STRING,
  city STRING,
  state_province STRING,
  country STRING,
  description STRING,
  sk_company_id BIGINT,
  batch_id INT,
  load_timestamp TIMESTAMP
)
USING DELTA;
```

---

## silver_securities
**Source**: bronze_finwire (SEC records).

```sql
CREATE TABLE silver_securities (
  pts STRING,
  symbol STRING,
  issue_type STRING,
  status STRING,
  name STRING,
  ex_id STRING,
  sh_out BIGINT,
  first_trade_date STRING,
  first_trade_exchg STRING,
  dividend DOUBLE,
  co_name_or_cik STRING,
  batch_id INT,
  load_timestamp TIMESTAMP
)
USING DELTA;
```

---

## silver_financials
**Source**: bronze_finwire (FIN records).

```sql
CREATE TABLE silver_financials (
  pts STRING,
  year INT,
  quarter INT,
  qtr_start_date STRING,
  posting_date STRING,
  revenue DOUBLE,
  earnings DOUBLE,
  eps DOUBLE,
  diluted_eps DOUBLE,
  margin DOUBLE,
  inventory DOUBLE,
  assets DOUBLE,
  liabilities DOUBLE,
  sh_out BIGINT,
  diluted_sh_out BIGINT,
  co_name_or_cik STRING,
  batch_id INT,
  load_timestamp TIMESTAMP
)
USING DELTA;
```

---

## silver_date
**Source**: bronze_date.

```sql
CREATE TABLE silver_date (
  sk_date_id INT,
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
  batch_id INT
)
USING DELTA;
```

---

## silver_status_type
**Source**: bronze_status_type.

```sql
CREATE TABLE silver_status_type (
  st_id STRING,
  st_name STRING,
  batch_id INT,
  load_timestamp TIMESTAMP
)
USING DELTA;
```

---

## silver_trade_type
**Source**: bronze_trade_type.

```sql
CREATE TABLE silver_trade_type (
  tt_id STRING,
  tt_name STRING,
  tt_is_sell BOOLEAN,
  tt_is_mrkt BOOLEAN,
  batch_id INT,
  load_timestamp TIMESTAMP
)
USING DELTA;
```

---

## silver_industry
**Source**: bronze_industry. Spec 2.2.11: IN_ID|IN_NAME|IN_SC_ID (pipe, 3 columns only).

```sql
CREATE TABLE silver_industry (
  in_id STRING,
  in_name STRING,
  in_sc_id STRING,
  batch_id INT,
  load_timestamp TIMESTAMP
)
USING DELTA;
```

---

## silver_tax_rate
**Source**: bronze_tax_rate.

```sql
CREATE TABLE silver_tax_rate (
  tx_id STRING,
  tx_name STRING,
  tx_rate DOUBLE,
  batch_id INT,
  load_timestamp TIMESTAMP
)
USING DELTA;
```
