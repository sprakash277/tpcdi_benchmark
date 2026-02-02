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
