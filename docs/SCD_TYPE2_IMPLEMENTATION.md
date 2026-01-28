# SCD Type 2 Implementation for Incremental Loads

This document explains how Slowly Changing Dimension Type 2 (SCD Type 2) is implemented for incremental batches (Batch 2, Batch 3, etc.) in the TPC-DI benchmark.

## Table of Contents

1. [Overview](#overview)
2. [Batch 1 vs Batch 2+](#batch-1-vs-batch-2)
3. [Code Flow](#code-flow)
4. [Visual Flow Diagram](#visual-flow-diagram)
5. [Detailed Examples](#detailed-examples)
6. [Edge Cases](#edge-cases)
7. [Key Implementation Details](#key-implementation-details)
8. [Testing & Validation](#testing--validation)
9. [Troubleshooting](#troubleshooting)
10. [Performance Considerations](#performance-considerations)

## Overview

SCD Type 2 maintains a complete history of dimension changes by:
1. **Closing out** old versions of records (setting `is_current=false`, `end_date`)
2. **Inserting** new versions with `is_current=true` and `effective_date`

This allows you to query the dimension "as of" any point in time.

### Why SCD Type 2?

- **Historical Analysis**: Track how customer/account attributes changed over time
- **Audit Trail**: Know exactly when changes occurred
- **Time-Travel Queries**: Query dimension state at any historical point
- **Compliance**: Maintain complete change history for regulatory requirements

## Batch 1 vs Batch 2+

### Batch 1 (Historical Load)

**Behavior**: Full overwrite, no SCD Type 2 logic
- All records inserted with `is_current=true`, `end_date=NULL`
- No MERGE operations
- Establishes baseline state

**Code Path:**
```python
# In customers.py or accounts.py
if batch_id == 1:
    return self._write_silver_table(silver_df, target_table, batch_id)
    # Simple overwrite, no SCD logic
```

### Batch 2+ (Incremental Load)

**Behavior**: SCD Type 2 MERGE
- Closes existing current records
- Inserts new versions
- Maintains complete history

**Code Path:**
```python
# In customers.py or accounts.py
else:
    return self._apply_scd_type2(...)
    # Applies SCD Type 2 logic
```

## Visual Flow Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Incremental Batch Load                    │
│                      (Batch 2, Batch 3)                     │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Silver Loader (customers.py / accounts.py)                 │
│  - Parses incoming data (XML or pipe-delimited)              │
│  - Transforms to silver schema                               │
│  - Calls _apply_scd_type2()                                  │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  _apply_scd_type2() in base.py                              │
│                                                              │
│  Step 1: Check if target table exists                       │
│          ├─ No → Create with overwrite                      │
│          └─ Yes → Continue                                  │
│                            │                                 │
│  Step 2: Create temp views                                  │
│          - incoming_changes (all incoming data)              │
│          - incoming_keys (distinct business keys)            │
│                            │                                 │
│  Step 3: MERGE (Close old records)                           │
│          ┌────────────────────────────────────┐            │
│          │ MERGE INTO target                   │            │
│          │ USING incoming_changes              │            │
│          │ ON business_key match               │            │
│          │   AND is_current = true             │            │
│          │ WHEN MATCHED THEN                   │            │
│          │   UPDATE SET                        │            │
│          │     is_current = false              │            │
│          │     end_date = new_effective_date   │            │
│          └────────────────────────────────────┘            │
│                            │                                 │
│  Step 4: INSERT (Add new versions)                          │
│          - Append all incoming records                       │
│          - is_current = true                                 │
│          - end_date = NULL                                   │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│              Updated Silver Table                            │
│  - Old version: is_current=false, end_date set               │
│  - New version: is_current=true, end_date=NULL              │
└─────────────────────────────────────────────────────────────┘
```

## Code Flow

### 1. Entry Point: Silver Loaders

When processing incremental batches, the Silver loaders call `_apply_scd_type2()`:

**For Customers** (`benchmark/etl/silver/customers.py`):
```python
# Batch 2+: Incremental CDC with SCD Type 2
return self._apply_scd_type2(
    incoming_df=silver_df,
    target_table=target_table,
    key_column="customer_id",        # Business key to match on
    effective_date_col="effective_date"
)
```

**For Accounts** (`benchmark/etl/silver/accounts.py`):
```python
# Batch 2+: Incremental CDC with SCD Type 2
return self._apply_scd_type2(
    incoming_df=silver_df,
    target_table=target_table,
    key_column="account_id",         # Business key to match on
    effective_date_col="effective_date"
)
```

### 2. Core Method: `_apply_scd_type2()`

Located in `benchmark/etl/silver/base.py`, this method implements the SCD Type 2 logic:

```python
def _apply_scd_type2(self, incoming_df: DataFrame, target_table: str,
                     key_column: str, effective_date_col: str = "effective_date") -> DataFrame:
```

#### Step-by-Step Process:

**Step 1: Check if table exists**
```python
try:
    existing_df = self.spark.table(target_table)
    table_exists = True
except Exception:
    table_exists = False
```

If table doesn't exist (shouldn't happen for Batch 2+, but safety check), it creates it with overwrite.

**Step 2: Create temporary views**
```python
incoming_df.createOrReplaceTempView("incoming_changes")
incoming_keys = incoming_df.select(key_column).distinct()
incoming_keys.createOrReplaceTempView("incoming_keys")
```

**Step 3: Close out existing current records (MERGE)**

This is the critical step. It uses Delta Lake MERGE to update existing records:

```sql
MERGE INTO silver_customers AS target
USING (
    SELECT customer_id, MIN(effective_date) as new_effective_date
    FROM incoming_changes
    GROUP BY customer_id
) AS updates
ON target.customer_id = updates.customer_id AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date
```

**What this does:**
- Finds all incoming customer IDs that have updates
- For each customer ID, gets the minimum `effective_date` (earliest change)
- Matches against existing records where:
  - `customer_id` matches
  - `is_current = true` (only close out current versions)
- Updates matched records:
  - Sets `is_current = false`
  - Sets `end_date = new_effective_date` (when the new version becomes effective)

**Step 4: Insert new versions**
```python
self.platform.write_table(incoming_df, target_table, mode="append")
```

All incoming records are appended as new versions with:
- `is_current = true`
- `effective_date` = from incoming data
- `end_date = NULL`

## Detailed Examples

### Example 1: Single Customer Update

### Before Batch 2 (After Batch 1)

**silver_customers table:**
| customer_id | last_name | city | is_current | effective_date | end_date |
|-------------|-----------|------|------------|----------------|----------|
| 12345 | Smith | New York | true | 2007-01-01 | NULL |

### Batch 2: Customer 12345 moves to Los Angeles

**Incoming data (from Customer.txt):**
| customer_id | last_name | city | effective_date |
|-------------|-----------|------|----------------|
| 12345 | Smith | Los Angeles | 2007-07-15 |

### Step 1: MERGE closes out old record

**MERGE statement executes:**
```sql
MERGE INTO silver_customers AS target
USING (
    SELECT customer_id, MIN(effective_date) as new_effective_date
    FROM incoming_changes
    GROUP BY customer_id
) AS updates
ON target.customer_id = updates.customer_id AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date
```

**Result after MERGE:**
| customer_id | last_name | city | is_current | effective_date | end_date |
|-------------|-----------|------|------------|----------------|----------|
| 12345 | Smith | New York | **false** | 2007-01-01 | **2007-07-15** |

### Step 2: Append new version

**After append:**
| customer_id | last_name | city | is_current | effective_date | end_date |
|-------------|-----------|------|------------|----------------|----------|
| 12345 | Smith | New York | false | 2007-01-01 | 2007-07-15 |
| 12345 | Smith | **Los Angeles** | **true** | **2007-07-15** | **NULL** |

### Querying "As Of" a Date

**Query: "What was customer 12345's city on 2007-06-01?"**
```sql
SELECT city 
FROM silver_customers 
WHERE customer_id = 12345 
  AND '2007-06-01' >= effective_date 
  AND ('2007-06-01' < end_date OR end_date IS NULL)
```
**Result:** New York (the old version was still current)

**Query: "What is customer 12345's current city?"**
```sql
SELECT city 
FROM silver_customers 
WHERE customer_id = 12345 
  AND is_current = true
```
**Result:** Los Angeles (the new version)

### Example 2: Multiple Updates in Same Batch

**Scenario**: Customer 12345 appears twice in Batch 2 (address change, then status change)

**Incoming data:**
| customer_id | city | status | effective_date |
|-------------|------|--------|----------------|
| 12345 | Los Angeles | ACTIVE | 2007-07-15 |
| 12345 | Los Angeles | INACT | 2007-07-20 |

**MERGE uses MIN(effective_date) = 2007-07-15**

**Result:**
| customer_id | city | status | is_current | effective_date | end_date |
|-------------|------|--------|-------------|----------------|----------|
| 12345 | New York | ACTIVE | false | 2007-01-01 | **2007-07-15** |
| 12345 | Los Angeles | ACTIVE | false | 2007-07-15 | 2007-07-20 |
| 12345 | Los Angeles | INACT | true | 2007-07-20 | NULL |

**Key Point**: The old record's `end_date` is set to the **earliest** change date (2007-07-15), ensuring no gaps in history.

### Example 3: New Customer (No Existing Record)

**Scenario**: Customer 99999 is new in Batch 2

**Incoming data:**
| customer_id | last_name | city | effective_date |
|-------------|-----------|------|----------------|
| 99999 | Johnson | Chicago | 2007-07-15 |

**MERGE**: No match (customer_id doesn't exist), so MERGE does nothing

**Append**: New record inserted

**Result:**
| customer_id | last_name | city | is_current | effective_date | end_date |
|-------------|-----------|------|-------------|----------------|----------|
| 99999 | Johnson | Chicago | true | 2007-07-15 | NULL |

### Example 4: Account Inactivation

**Scenario**: Account 50000 is inactivated in Batch 2

**Before Batch 2:**
| account_id | account_desc | is_active | is_current | effective_date | end_date |
|------------|--------------|-----------|-------------|----------------|----------|
| 50000 | Investment Account | true | true | 2007-01-01 | NULL |

**Batch 2 incoming:**
| account_id | account_desc | status | effective_date |
|------------|--------------|--------|----------------|
| 50000 | Investment Account | INACT | 2007-07-15 |

**After SCD Type 2:**
| account_id | account_desc | is_active | is_current | effective_date | end_date |
|------------|--------------|-----------|-------------|----------------|----------|
| 50000 | Investment Account | true | false | 2007-01-01 | 2007-07-15 |
| 50000 | Investment Account | **false** | true | 2007-07-15 | NULL |

## Edge Cases

### Edge Case 1: Record Already Closed

**Scenario**: Customer 12345 was already closed in Batch 2, but appears again in Batch 3

**Before Batch 3:**
| customer_id | city | is_current | effective_date | end_date |
|-------------|------|------------|----------------|----------|
| 12345 | New York | false | 2007-01-01 | 2007-07-15 |
| 12345 | Los Angeles | true | 2007-07-15 | NULL |

**Batch 3 incoming:**
| customer_id | city | effective_date |
|-------------|------|----------------|
| 12345 | San Francisco | 2007-08-01 |

**MERGE**: Only matches `is_current=true` record (Los Angeles version)
- The already-closed New York version is NOT touched

**Result:**
| customer_id | city | is_current | effective_date | end_date |
|-------------|------|------------|----------------|----------|
| 12345 | New York | false | 2007-01-01 | 2007-07-15 |
| 12345 | Los Angeles | false | 2007-07-15 | 2007-08-01 |
| 12345 | San Francisco | true | 2007-08-01 | NULL |

### Edge Case 2: Duplicate Records in Incoming Batch

**Scenario**: Same customer appears twice with same effective_date

**Incoming data:**
| customer_id | city | effective_date |
|-------------|------|----------------|
| 12345 | Los Angeles | 2007-07-15 |
| 12345 | Los Angeles | 2007-07-15 |

**MERGE**: `GROUP BY customer_id, MIN(effective_date)` handles duplicates
- Only one update record created
- Old record closed once

**Append**: Both records inserted (may create duplicates)
- **Note**: This is a data quality issue that should be handled upstream
- Consider adding deduplication before calling `_apply_scd_type2()`

### Edge Case 3: Effective Date Before Existing Record

**Scenario**: Incoming record has effective_date earlier than existing record

**Before Batch 2:**
| customer_id | city | effective_date | end_date |
|-------------|------|----------------|----------|
| 12345 | New York | 2007-07-01 | NULL |

**Batch 2 incoming:**
| customer_id | city | effective_date |
|-------------|------|----------------|
| 12345 | Los Angeles | 2007-06-15 | ← Earlier date!

**MERGE**: Still closes current record (2007-07-01)
- Sets `end_date = 2007-06-15`

**Append**: Inserts new record with `effective_date = 2007-06-15`

**Result:**
| customer_id | city | is_current | effective_date | end_date |
|-------------|------|------------|----------------|----------|
| 12345 | New York | false | 2007-07-01 | 2007-06-15 | ← Time gap!
| 12345 | Los Angeles | true | 2007-06-15 | NULL |

**Issue**: Creates a time gap (Los Angeles effective before New York ends)
- **Note**: This indicates data quality issue - should be validated upstream
- Current implementation doesn't prevent this (follows TPC-DI spec as-is)

## Key Implementation Details

### 1. Business Key Matching

The method uses the **business key** (`customer_id`, `account_id`) to identify which records to update, not a surrogate key. This is correct for SCD Type 2 because:
- Multiple versions of the same business entity share the same business key
- The business key is stable across time

### 2. Only Closes Current Records

The MERGE condition includes `target.is_current = true`:
```sql
ON target.customer_id = updates.customer_id AND target.is_current = true
```

This ensures:
- Only the **current** version is closed out
- Historical versions (already closed) are not touched
- If a record was already closed, it won't be updated again

### 3. MIN(effective_date) for Multiple Updates

If the same customer appears multiple times in the incoming batch, we use `MIN(effective_date)`:
```sql
SELECT customer_id, MIN(effective_date) as new_effective_date
FROM incoming_changes
GROUP BY customer_id
```

This ensures:
- The `end_date` of the old record is set to the **earliest** change date
- All versions in the batch become effective at the correct time

### 4. Delta Lake MERGE

The implementation uses Delta Lake's `MERGE INTO` statement, which:
- Is atomic (all-or-nothing)
- Is efficient (only updates matching rows)
- Supports concurrent writes

**Fallback:** If MERGE fails (non-Delta table), it falls back to append-only mode (less efficient but still works).

### 5. New Records vs. Updates

The same logic handles both:
- **New records** (customer_id not in table): MERGE doesn't match, so they're just inserted
- **Updated records** (customer_id exists): MERGE closes old version, then new version is inserted

## Code Location Summary

| Component | File | Method | Purpose |
|-----------|------|--------|---------|
| Base SCD Type 2 logic | `benchmark/etl/silver/base.py` | `_apply_scd_type2()` | Core MERGE logic |
| Customer SCD Type 2 | `benchmark/etl/silver/customers.py` | `load()` → `_apply_scd_type2()` | Calls base method for customers |
| Account SCD Type 2 | `benchmark/etl/silver/accounts.py` | `load()` → `_apply_scd_type2()` | Calls base method for accounts |

## Testing & Validation

### Validation Queries

To verify SCD Type 2 is working correctly:

1. **Check current records:**
   ```sql
   SELECT COUNT(*) FROM silver_customers WHERE is_current = true
   ```

2. **Check historical records:**
   ```sql
   SELECT COUNT(*) FROM silver_customers WHERE is_current = false
   ```

3. **Check version history for a customer:**
   ```sql
   SELECT customer_id, city, is_current, effective_date, end_date
   FROM silver_customers
   WHERE customer_id = 12345
   ORDER BY effective_date
   ```

4. **Verify no gaps in time:**
   ```sql
   -- Should return 0 rows (no gaps)
   SELECT a.customer_id, a.end_date, b.effective_date
   FROM silver_customers a
   JOIN silver_customers b ON a.customer_id = b.customer_id
   WHERE a.end_date IS NOT NULL
     AND b.effective_date > a.end_date
     AND NOT EXISTS (
       SELECT 1 FROM silver_customers c
       WHERE c.customer_id = a.customer_id
         AND c.effective_date > a.end_date
         AND c.effective_date < b.effective_date
     )
   ```

5. **Verify exactly one current record per business key:**
   ```sql
   -- Should return 0 rows (each customer has exactly one current record)
   SELECT customer_id, COUNT(*) as current_count
   FROM silver_customers
   WHERE is_current = true
   GROUP BY customer_id
   HAVING COUNT(*) > 1
   ```

6. **Verify end_date is set when is_current=false:**
   ```sql
   -- Should return 0 rows (all closed records have end_date)
   SELECT customer_id, effective_date, end_date
   FROM silver_customers
   WHERE is_current = false AND end_date IS NULL
   ```

7. **Verify end_date is NULL when is_current=true:**
   ```sql
   -- Should return 0 rows (all current records have NULL end_date)
   SELECT customer_id, effective_date, end_date
   FROM silver_customers
   WHERE is_current = true AND end_date IS NOT NULL
   ```

8. **Check version count per customer:**
   ```sql
   -- Shows how many versions each customer has
   SELECT customer_id, COUNT(*) as version_count,
          MIN(effective_date) as first_version,
          MAX(CASE WHEN is_current THEN effective_date END) as current_version_date
   FROM silver_customers
   GROUP BY customer_id
   ORDER BY version_count DESC
   LIMIT 10
   ```

## Troubleshooting

### Issue 1: MERGE Fails with "Table not found"

**Symptoms:**
```
AnalysisException: Table or view not found: silver_customers
```

**Cause**: Table doesn't exist (shouldn't happen for Batch 2+)

**Solution**: 
- Check that Batch 1 was run successfully
- Verify table exists: `SHOW TABLES LIKE 'silver_customers'`
- If missing, run Batch 1 first

### Issue 2: Multiple Current Records for Same Customer

**Symptoms:**
```sql
SELECT customer_id, COUNT(*) FROM silver_customers 
WHERE is_current = true GROUP BY customer_id HAVING COUNT(*) > 1
-- Returns rows
```

**Cause**: 
- MERGE didn't execute (non-Delta table fallback)
- Concurrent writes
- Data quality issue (duplicate incoming records)

**Solution**:
- Verify table is Delta format: `DESCRIBE EXTENDED silver_customers`
- Check for concurrent writes
- Add deduplication before `_apply_scd_type2()`

### Issue 3: Records Not Being Closed

**Symptoms**: Old records remain `is_current=true` after incremental load

**Cause**:
- MERGE condition not matching (`is_current=true` check)
- Business key mismatch
- MERGE failed silently

**Solution**:
- Check MERGE execution logs
- Verify business key values match
- Test MERGE manually:
  ```sql
  MERGE INTO silver_customers AS target
  USING (SELECT customer_id, MIN(effective_date) as new_date
         FROM incoming_changes GROUP BY customer_id) AS updates
  ON target.customer_id = updates.customer_id AND target.is_current = true
  WHEN MATCHED THEN UPDATE SET target.is_current = false, target.end_date = updates.new_date
  ```

### Issue 4: Performance Issues

**Symptoms**: Incremental loads are slow

**Causes & Solutions**:
- **Large incoming batches**: Consider partitioning by business key
- **Many updates**: MERGE scans entire table - ensure proper indexing
- **Non-Delta table**: Convert to Delta for better MERGE performance
- **Concurrent writes**: Use Delta Lake's optimistic concurrency control

**Optimization Tips**:
```python
# Add partitioning if needed
self.platform.write_table(df, table, mode="append", partition_by=["customer_id"])

# Use Delta Lake optimizations
spark.sql("OPTIMIZE silver_customers")
spark.sql("VACUUM silver_customers RETAIN 168 HOURS")
```

## Performance Considerations

### MERGE Performance

**Delta Lake MERGE is efficient because:**
- Uses indexes/statistics to find matching rows
- Only updates rows that match (doesn't scan entire table)
- Atomic operation (no intermediate states)

**Performance factors:**
- **Table size**: Larger tables = longer MERGE
- **Match rate**: More matches = more updates
- **Partitioning**: Partitioning by business key can improve performance
- **Z-ordering**: `OPTIMIZE` with Z-ordering can improve MERGE performance

### Best Practices

1. **Run OPTIMIZE regularly:**
   ```sql
   OPTIMIZE silver_customers ZORDER BY (customer_id)
   ```

2. **Monitor MERGE performance:**
   ```python
   import time
   start = time.time()
   self._apply_scd_type2(...)
   duration = time.time() - start
   logger.info(f"SCD Type 2 took {duration:.2f} seconds")
   ```

3. **Batch size considerations:**
   - Smaller batches = more frequent MERGEs = more overhead
   - Larger batches = fewer MERGEs but more data per MERGE
   - Balance based on your SLA requirements

4. **Consider partitioning:**
   - If table is very large (>100M rows), consider partitioning
   - Partition by business key or date range
   - Helps MERGE find matching rows faster

## Quick Reference

### When SCD Type 2 is Applied

| Batch | Format | SCD Type 2? | Method |
|-------|--------|-------------|--------|
| Batch 1 | XML (CustomerMgmt.xml) | ❌ No | `_write_silver_table()` with `mode="overwrite"` |
| Batch 2+ | Pipe-delimited (Customer.txt, Account.txt) | ✅ Yes | `_apply_scd_type2()` with MERGE |

### Key Columns

| Column | Type | Purpose | Values |
|--------|------|---------|--------|
| `is_current` | Boolean | Indicates current version | `true` = current, `false` = historical |
| `effective_date` | Timestamp | When version became active | From source data |
| `end_date` | Timestamp | When version was superseded | `NULL` for current, timestamp for historical |
| `customer_id` / `account_id` | Long | Business key | Used for matching records |

### MERGE Statement Template

```sql
MERGE INTO {target_table} AS target
USING (
    SELECT {key_column}, MIN({effective_date_col}) as new_effective_date
    FROM incoming_changes
    GROUP BY {key_column}
) AS updates
ON target.{key_column} = updates.{key_column} AND target.is_current = true
WHEN MATCHED THEN UPDATE SET
    target.is_current = false,
    target.end_date = updates.new_effective_date
```

### Common Queries

**Get current version:**
```sql
SELECT * FROM silver_customers WHERE customer_id = 12345 AND is_current = true
```

**Get all versions:**
```sql
SELECT * FROM silver_customers WHERE customer_id = 12345 ORDER BY effective_date
```

**Get version as of date:**
```sql
SELECT * FROM silver_customers 
WHERE customer_id = 12345 
  AND '2007-07-01' >= effective_date 
  AND ('2007-07-01' < end_date OR end_date IS NULL)
```

**Get change history:**
```sql
SELECT customer_id, city, effective_date, end_date,
       DATEDIFF(end_date, effective_date) as days_active
FROM silver_customers
WHERE customer_id = 12345
ORDER BY effective_date
```

## Summary

The SCD Type 2 implementation:
1. ✅ Closes out existing current records when updates arrive
2. ✅ Inserts new versions with proper timestamps
3. ✅ Handles both new records and updates
4. ✅ Uses efficient Delta Lake MERGE operations
5. ✅ Maintains complete history for time-travel queries
6. ✅ Handles edge cases (duplicates, already-closed records, etc.)
7. ✅ Provides fallback for non-Delta tables

This ensures that incremental loads (Batch 2, Batch 3) properly maintain dimension history according to SCD Type 2 principles, enabling:
- **Historical analysis**: Track how dimensions changed over time
- **Time-travel queries**: Query dimension state at any point in history
- **Audit compliance**: Complete change history for regulatory requirements
- **Data quality**: Detect and handle data anomalies

## Related Documentation

- [Medallion Architecture](./MEDALLION_ARCHITECTURE.md) - Overall architecture overview
- [CustomerMgmt Structure](./CUSTOMERMGMT_STRUCTURE.md) - XML structure details
- [TPC-DI Specification](https://www.tpc.org/tpcdi/) - Official benchmark specification
