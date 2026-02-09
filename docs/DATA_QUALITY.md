# Data Quality (DQ) at Silver and Late-Arriving Dimensions

TPC-DI validation rules run at the Silver layer; failures are logged to **gold_dim_messages** (DimMessages). Late-arriving facts are handled in Gold with placeholder dimension rows and a flag.

---

## DimMessages (gold_dim_messages)

TPC-DI audit table for DQ failures. Every Silver rule that triggers inserts a row.

| Column              | Type     | Description                                |
|---------------------|----------|--------------------------------------------|
| message_timestamp   | TIMESTAMP| When the message was logged                |
| batch_id            | INT      | Batch number                               |
| component_name      | STRING   | e.g. `Silver_Customer_Validation`           |
| message_text        | STRING   | e.g. `customer_id/tax_id NULL: 5 row(s)`   |
| severity            | STRING   | `Alert` or `Reject`                        |
| source_table        | STRING   | Silver table validated (e.g. silver_customers) |

The table is created on first use (by Silver DQ or at Gold load start).

---

## Mandatory TPC-DI Validation Rules (Silver)

| Table / Rule        | Category   | DQ Expectation                                      | Severity |
|---------------------|------------|------------------------------------------------------|----------|
| DimCustomer         | Null checks| customer_id IS NOT NULL AND tax_id IS NOT NULL       | Reject   |
| DimCustomer         | Value range| tier IN (1, 2, 3)                                   | Alert    |
| DimCustomer         | Validity   | dob must be in the past                              | Alert    |
| DimAccount          | RI         | customer_id must exist in silver_customers           | Alert    |
| DimAccount          | Completeness | customer_id IS NOT NULL                             | Reject   |
| FactTrades          | Logic      | bid_price > 0 AND quantity > 0                       | Alert    |
| DimDate             | Format     | sk_date_id valid YYYYMMDD (8-digit)                  | Alert    |

---

## Extended Silver DQ Rules (Additional Tables and Checks)

Beyond the mandatory TPC-DI rules, the following are also run:

| Table | Rule | Severity |
|-------|------|----------|
| **silver_customers** | gender in (M, F, U) when present | Alert |
| **silver_customers** | status non-null and non-empty | Alert |
| **silver_accounts** | account_id IS NOT NULL | Reject |
| **silver_trades** | account_id IS NOT NULL | Reject |
| **silver_trades** | trade_price > 0 when present | Alert |
| **silver_trades** | commission >= 0, tax >= 0 | Alert |
| **silver_securities** | symbol non-null and non-empty | Alert |
| **silver_securities** | duplicate symbol (uniqueness) | Alert |
| **silver_daily_market** | dm_date IS NOT NULL | Alert |
| **silver_daily_market** | dm_close, dm_high, dm_low, dm_vol >= 0 | Alert |
| **silver_cash_transaction** | account_id / ct_ca_id IS NOT NULL | Reject |
| **silver_cash_transaction** | transaction_date / ct_dts IS NOT NULL | Alert |
| **silver_status_type** | st_id, st_name non-null and non-empty | Alert |
| **silver_trade_type** | tt_id, tt_name non-null and non-empty | Alert |
| **silver_industry** | in_id, in_name non-null and non-empty | Alert |

---

## Generic Silver DQ Checks

- **Completeness**: NULLs in keys (customer_id, tax_id, etc.) → logged to DimMessages.
- **Uniqueness**: Duplicate `trade_id` or `customer_id` within the same batch → Alert.
- **Validity**: `dob` must be in the past → Alert.
- **Consistency**: `end_date` must be >= `effective_date` (silver_customers, silver_accounts) → Alert.

---

## Late-Arriving Dimension (TPC-DI)

**Problem**: A trade arrives in Batch 2 but its Account (or Customer) does not arrive until Batch 3.

**Behavior**:

1. **Placeholder rows**
   - **gold_dim_customer**: one row with `customer_id = -1`, `sk_customer_id = -1` (Unknown).
   - **gold_dim_account**: one row with `account_id = -1`, `sk_account_id = -1`, `customer_id = -1` (Unknown).

2. **FactTrade**
   - Joins to dimensions are **left** joins.
   - Missing dimension keys are filled with **-1** (placeholder).
   - **late_arriving_flag** is set to `true` when `sk_account_id` or `sk_customer_id` from the join is NULL (i.e. the trade referenced an account/customer not yet in the dimension).

Trades are not dropped; they are written with placeholder keys and flagged for later reconciliation.

---

## Execution

- **Silver DQ**: Runs automatically after each Silver batch load (`run_silver_batch_load`). Uses `SilverDQRunner.run_silver_dq(batch_id, prefix)` and writes to `{prefix}.gold_dim_messages`.
- **Gold**: Ensures `gold_dim_messages` exists at Gold load start. DimCustomer and DimAccount loads add the placeholder rows; FactTrade adds `late_arriving_flag`.

---

## Adding more complex DQ rules

You can extend DQ in three ways: add checks inside existing rule methods, add new rule methods and call them from `run_silver_dq`, or register custom rule functions (e.g. cross-table, regex, window-based).

### 1. Add checks in existing methods

In `benchmark/etl/dq/silver_rules.py`, inside `_run_customer_rules`, `_run_account_rules`, `_run_trade_rules`, or `_run_date_rules`, add more filters and call `log(...)` when the check fails.

**Example — regex on a column (e.g. tax_id format):**

```python
from pyspark.sql.functions import col, regexp_extract

# In _run_customer_rules, after existing checks:
bad_tax = df.filter(
    col("tax_id").isNotNull() & ~col("tax_id").rlike(r"^[A-Z0-9\-]+$")
)
n = bad_tax.count()
if n > 0:
    log("Silver_Customer_Validation", f"tax_id invalid format: {n} row(s)", "Alert", source)
```

**Example — multi-column rule (e.g. effective_date <= end_date):**

```python
bad_range = df.filter(
    col("effective_date").isNotNull() & col("end_date").isNotNull()
    & (col("effective_date") > col("end_date"))
)
if bad_range.count() > 0:
    log("Silver_Customer_Validation", "effective_date > end_date", "Alert", source)
```

### 2. Add a new rule method and call it from run_silver_dq

1. In `silver_rules.py`, add a method (e.g. `_run_security_rules`) that:
   - Reads the silver table with `self.spark.table(f"{prefix}.silver_securities")`.
   - Optionally filters by `batch_id` if the table has that column.
   - Runs your checks (filter + count or aggregations).
   - Calls `log(component_name, message_text, severity, source_table)` for each failure.
2. In `run_silver_dq`, add a `try`/`except` block that calls your new method (same pattern as customer/account/trade/date).

**Example — new table (silver_securities):**

```python
def _run_security_rules(self, prefix: str, batch_id: int, log, messages_table: str) -> None:
    source = f"{prefix}.silver_securities"
    try:
        df = self.spark.table(source)
    except Exception:
        logger.debug(f"Table {source} not found, skipping security DQ")
        return
    if "batch_id" in df.columns:
        df = df.filter(col("batch_id") == batch_id)
    # Example: symbol must be non-empty
    from pyspark.sql.functions import trim
    bad = df.filter(col("symbol").isNull() | (trim(col("symbol")) == ""))
    n = bad.count()
    if n > 0:
        log("Silver_Security_Validation", f"symbol NULL or empty: {n} row(s)", "Alert", source)
```

Then in `run_silver_dq`:

```python
try:
    self._run_security_rules(prefix, batch_id, log, messages_table)
except Exception as e:
    logger.warning(f"Silver DQ silver_securities failed: {e}")
    log("Silver_Security_Validation", f"DQ run failed: {e}", "Alert", f"{prefix}.silver_securities")
```

### 3. Per-row or bulk messages (log_messages)

When a rule has many failures and you want one DimMessages row per bad row (or per key), build a DataFrame with columns `message_timestamp`, `batch_id`, `component_name`, `message_text`, `severity`, `source_table` and use `log_messages` from `benchmark.etl.dq.dim_messages`.

**Example — one message per duplicate key with key value:**

```python
from pyspark.sql.functions import current_timestamp, lit, count, concat
from benchmark.etl.dq.dim_messages import log_messages

# In your rule method:
dup = df.groupBy("customer_id").agg(count("*").alias("cnt")).filter(col("cnt") > 1)
if dup.count() > 0:
    messages_df = dup.select(
        current_timestamp().alias("message_timestamp"),
        lit(batch_id).alias("batch_id"),
        lit("Silver_Customer_Validation").alias("component_name"),
        concat(lit("duplicate customer_id: "), col("customer_id").cast("string")).alias("message_text"),
        lit("Alert").alias("severity"),
        lit(source).alias("source_table"),
    )
    log_messages(self.spark, self.platform, messages_table, messages_df)
```

### 4. Custom rule functions (without editing silver_rules.py)

You can pass a list of custom rule functions into `run_silver_dq(..., custom_rules=...)`. Each function is called as:

`fn(runner, prefix, batch_id, log, messages_table)`

where `runner` is the `SilverDQRunner` instance (use `runner.spark`, `runner.platform`), `log` is the same `log(component, message, severity, source)` used by built-in rules, and `messages_table` is the full table name for DimMessages.

**Example — custom rule and wiring from your code:**

```python
def my_custom_rule(runner, prefix: str, batch_id: int, log, messages_table: str) -> None:
    df = runner.spark.table(f"{prefix}.silver_customers")
    if "batch_id" in df.columns:
        df = df.filter(col("batch_id") == batch_id)
    # Your logic; call log() for each failure
    bad = df.filter(some_complex_condition)
    if bad.count() > 0:
        log("My_Custom_Validation", "Description of failure", "Alert", f"{prefix}.silver_customers")

# When calling run_silver_dq (e.g. from silver/__init__.py or your ETL):
dq_runner.run_silver_dq(batch_id, prefix, custom_rules=[my_custom_rule])
```

Use `custom_rules` for project-specific or experimental rules without changing the core `silver_rules.py` file.
