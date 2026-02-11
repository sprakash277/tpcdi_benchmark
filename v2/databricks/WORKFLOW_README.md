# TPC-DI v2 Databricks Workflow Guide

## Overview

This workflow executes the TPC-DI v2 SQL-only implementation with separate tasks for each table creation and data load operation. It handles both batch and incremental loads automatically.

## Structure

The workflow is organized into three layers:

1. **Bronze Layer**: Raw data ingestion
   - Table creation tasks (one per table)
   - Data load tasks (batch vs incremental)

2. **Silver Layer**: Cleaned and conformed data
   - Table creation tasks (one per table)
   - Transformation tasks (batch vs incremental)

3. **Gold Layer**: Business-ready star schema
   - Table creation tasks (one per table)
   - Data load tasks (batch vs incremental)

## Creating the Workflow

### Method 1: Using Python Script

```bash
python v2/databricks/create_v2_workflow.py \
  --workspace-path "/Workspace/Repos/your-org/your-repo/v2/databricks" \
  --job-name "TPC-DI-v2-SQL" \
  --output v2_workflow.json \
  --databricks-host "https://your-workspace.cloud.databricks.com" \
  --databricks-token "dapi..." \
  --create-job
```

### Method 2: Manual JSON Creation

1. Run the script without `--create-job` to generate JSON:
```bash
python v2/databricks/create_v2_workflow.py \
  --workspace-path "/Workspace/Repos/your-org/your-repo/v2/databricks" \
  --output v2_workflow.json
```

2. Import JSON via Databricks UI:
   - Go to **Workflows** → **Jobs** → **Create Job**
   - Select **JSON** tab
   - Paste contents of `v2_workflow.json`
   - Click **Create**

## Workflow Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `raw_data_path` | `/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10` | TPC-DI raw data path |
| `catalog` | `tpcdi_catalog` | Unity Catalog name |
| `bronze_schema` | `bronze_schema` | Bronze schema name |
| `silver_schema` | `silver_schema` | Silver schema name |
| `gold_schema` | `gold_schema` | Gold schema name |
| `batch_id` | `1` | Batch ID (1 for batch, 2+ for incremental) |
| `load_type` | `batch` | Load type: `batch` or `incremental` |
| `warehouse_id` | `` | SQL Warehouse ID (required) |

## Running the Workflow

### Batch Load (Batch 1)

```bash
databricks jobs run-now \
  --job-id <job-id> \
  --notebook-params '{
    "batch_id": "1",
    "load_type": "batch",
    "raw_data_path": "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10",
    "warehouse_id": "your-warehouse-id"
  }'
```

### Incremental Load (Batch 2+)

```bash
databricks jobs run-now \
  --job-id <job-id> \
  --notebook-params '{
    "batch_id": "2",
    "load_type": "incremental",
    "raw_data_path": "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10",
    "warehouse_id": "your-warehouse-id"
  }'
```

## Task Execution Flow

### Batch 1 Execution

```
00_setup
  ├── bronze_create_* (all bronze tables)
  │   └── bronze_load_batch1
  │       └── silver_create_* (all silver tables)
  │           └── silver_transform_batch1
  │               └── gold_create_* (all gold tables)
  │                   └── gold_load_batch1
```

### Incremental Execution (Batch 2+)

```
00_setup
  ├── bronze_create_* (tables already exist, skipped if IF NOT EXISTS)
  │   └── bronze_load_incremental
  │       └── silver_create_* (tables already exist)
  │           └── silver_transform_incremental
  │               └── gold_create_* (tables already exist)
  │                   └── gold_load_incremental
```

## Conditional Task Execution

**Note**: Databricks SQL tasks don't support `run_if` conditions. The workflow includes both batch and incremental tasks. To handle batch vs incremental:

### Option 1: Separate Workflows (Recommended)
Create two workflows:
- **Batch Workflow**: Includes only batch tasks (bronze_load_batch1, silver_transform_batch1, gold_load_batch1)
- **Incremental Workflow**: Includes only incremental tasks (bronze_load_incremental, silver_transform_incremental, gold_load_incremental)

### Option 2: Single Workflow with Manual Task Disabling
- Create one workflow with all tasks
- For Batch 1: Disable incremental tasks manually in UI
- For Incremental: Disable batch tasks manually in UI

### Option 3: SQL-Level Conditional Logic
Modify SQL files to check `var.batch_id` and skip execution if not applicable:
```sql
-- In load/transform files, add:
SET var.batch_id = ${var.batch_id};
-- Then use IF statements or conditional logic in SQL
```

## Table Files Structure

Each table has its own SQL file:

```
v2/databricks/
├── setup.sql (generated)
├── bronze/
│   ├── tables/
│   │   ├── create_bronze_customer_mgmt.sql
│   │   ├── create_bronze_customer.sql
│   │   └── ... (one file per table)
│   ├── 02_load_bronze_batch1.sql
│   └── 03_load_bronze_incremental.sql
├── silver/
│   ├── tables/
│   │   ├── create_silver_customers.sql
│   │   └── ... (one file per table)
│   ├── 02_transform_silver_batch1.sql
│   └── 03_transform_silver_incremental.sql
└── gold/
    ├── tables/
    │   ├── create_gold_dim_customer.sql
    │   └── ... (one file per table)
    ├── 02_load_gold_batch1.sql
    └── 03_load_gold_incremental.sql
```

## Monitoring

1. **View in UI**: Go to **Workflows** → **Jobs** → Select job → View runs
2. **Check Task Status**: Each table creation and load task shows success/failure
3. **View Logs**: Click on any task to see SQL execution logs
4. **Verify Data**: Query tables to verify data loaded correctly

## Troubleshooting

### Task Fails: Table Already Exists
- This is expected for incremental loads
- Tables use `CREATE TABLE IF NOT EXISTS`, so they won't fail if they exist
- Check if the error is from a different issue

### Task Fails: File Not Found
- Verify `workspace_path` parameter matches your repo structure
- Ensure SQL files exist in the expected locations
- Check file paths in workflow JSON

### Task Fails: Warehouse Not Found
- Set `warehouse_id` parameter to a valid SQL Warehouse ID
- Find warehouse ID in **SQL** → **Warehouses** → Select warehouse → **Connection details**

### Conditional Tasks Not Running
- Check `run_if` conditions match your parameter values
- Verify `load_type` and `batch_id` are set correctly
- Review task dependencies

## Best Practices

1. **Use SQL Warehouse**: Configure a SQL Warehouse for better performance
2. **Monitor First Run**: Watch the first batch load to ensure all tasks complete
3. **Verify Data**: After each run, query sample tables to verify data
4. **Parameterize Paths**: Use workflow parameters for all configurable paths
5. **Schedule Incremental Loads**: Set up a schedule for regular incremental loads

## Example: Complete Batch 1 Run

```bash
# 1. Create workflow
python v2/databricks/create_v2_workflow.py \
  --workspace-path "/Workspace/Repos/org/repo/v2/databricks" \
  --job-name "TPC-DI-v2-Batch1" \
  --create-job \
  --databricks-host "https://workspace.cloud.databricks.com" \
  --databricks-token "dapi..."

# 2. Run Batch 1
databricks jobs run-now \
  --job-id <job-id> \
  --notebook-params '{
    "batch_id": "1",
    "load_type": "batch",
    "warehouse_id": "abc123..."
  }'

# 3. Run Batch 2 (incremental)
databricks jobs run-now \
  --job-id <job-id> \
  --notebook-params '{
    "batch_id": "2",
    "load_type": "incremental",
    "warehouse_id": "abc123..."
  }'
```
