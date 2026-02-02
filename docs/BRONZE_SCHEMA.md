# Bronze Layer Table Schemas

Schema definitions for all Bronze layer tables. Bronze captures raw source data with minimal transformation (metadata columns only).

Use `catalog.schema` prefix for Unity Catalog (e.g., `CREATE TABLE my_catalog.my_schema.bronze_trade`).

---

## Text-based Bronze Tables

These tables store one raw line per row. Source files are read as text; `raw_line` holds the entire line.

### bronze_trade
**Source**: Trade.txt (pipe-delimited)

```sql
CREATE TABLE bronze_trade (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_daily_market
**Source**: DailyMarket.txt (pipe-delimited)

```sql
CREATE TABLE bronze_daily_market (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_customer
**Source**: Customer.txt (pipe-delimited, Batch 2+ only)

```sql
CREATE TABLE bronze_customer (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_account
**Source**: Account.txt (pipe-delimited, Batch 2+ only)

```sql
CREATE TABLE bronze_account (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_prospect
**Source**: Prospect.csv (comma-delimited)

```sql
CREATE TABLE bronze_prospect (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_cash_transaction
**Source**: CashTransaction.txt (pipe-delimited)

```sql
CREATE TABLE bronze_cash_transaction (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_holding_history
**Source**: HoldingHistory.txt (pipe-delimited)

```sql
CREATE TABLE bronze_holding_history (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_watch_history
**Source**: WatchHistory.txt (pipe-delimited)

```sql
CREATE TABLE bronze_watch_history (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_finwire
**Source**: FINWIRE*.txt (fixed-width, Batch 1 only)

```sql
CREATE TABLE bronze_finwire (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_date
**Source**: Date.txt (pipe-delimited, Batch 1 only)

```sql
CREATE TABLE bronze_date (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_time
**Source**: Time.txt (pipe-delimited, Batch 1 only)

```sql
CREATE TABLE bronze_time (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_status_type
**Source**: StatusType.txt (pipe-delimited, Batch 1 only)

```sql
CREATE TABLE bronze_status_type (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_tax_rate
**Source**: TaxRate.txt (pipe-delimited, Batch 1 only)

```sql
CREATE TABLE bronze_tax_rate (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_trade_type
**Source**: TradeType.txt (pipe-delimited, Batch 1 only)

```sql
CREATE TABLE bronze_trade_type (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_industry
**Source**: Industry.txt (pipe-delimited, Batch 1 only)

```sql
CREATE TABLE bronze_industry (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

### bronze_hr
**Source**: HR.csv (comma-delimited, Batch 1 only)

```sql
CREATE TABLE bronze_hr (
  raw_line STRING,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

---

## bronze_customer_mgmt
**Source**: CustomerMgmt.xml (Batch 1 only)

Parsed via spark-xml into a nested struct. Schema varies by XML rowTag; typical structure includes flattened/nested fields from the XML.

```sql
CREATE TABLE bronze_customer_mgmt (
  -- XML-derived struct columns (exact schema depends on spark-xml parsing)
  -- Common fields after parse: _ActionType, _ActionTS, Customer (struct), etc.
  _ActionType STRING,
  _ActionTS STRING,
  Customer STRUCT<
    _C_ID: STRING,
    _C_TAX_ID: STRING,
    _C_GNDR: STRING,
    _C_TIER: STRING,
    _C_DOB: STRING,
    Name: STRUCT<C_L_NAME: STRING, C_F_NAME: STRING, C_M_NAME: STRING>,
    Address: STRUCT<C_ADLINE1: STRING, C_ADLINE2: STRING, C_ZIPCODE: STRING, C_CITY: STRING, C_STATE_PROV: STRING, C_CTRY: STRING>,
    ContactInfo: STRUCT<C_PRIM_EMAIL: STRING, C_ALT_EMAIL: STRING>,
    C_LCL_TX_ID: STRING,
    C_NAT_TX_ID: STRING,
    Account: STRUCT<_CA_ID: STRING, CA_B_ID: STRING, CA_NAME: STRING, _CA_TAX_ST: STRING>
  >,
  _load_timestamp TIMESTAMP,
  _source_file STRING,
  _batch_id INT
)
USING DELTA;
```

*Note: The actual bronze_customer_mgmt schema is inferred by spark-xml at read time. The struct above is illustrative; use `DESCRIBE bronze_customer_mgmt` after load to see the exact schema.*
