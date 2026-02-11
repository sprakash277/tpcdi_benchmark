# Quick Start: TPC-DI v2 Databricks Workflow

## Overview

The v2 implementation has been restructured with:
- **Individual table files**: Each table creation is in its own SQL file
- **Separate workflows**: Batch and Incremental workflows are separate
- **Variable-based**: All catalog/schema names use variables

## File Structure

```
v2/databricks/
├── bronze/
│   ├── tables/                    # Individual table creation files
│   │   ├── create_bronze_customer_mgmt.sql
│   │   ├── create_bronze_customer.sql
│   │   └── ... (17 files total)
│   ├── 02_load_bronze_batch1.sql
│   └── 03_load_bronze_incremental.sql
├── silver/
│   ├── tables/                    # Individual table creation files
│   │   ├── create_silver_customers.sql
│   │   └── ... (17 files total)
│   ├── 02_transform_silver_batch1.sql
│   └── 03_transform_silver_incremental.sql
├── gold/
│   ├── tables/                    # Individual table creation files
│   │   ├── create_gold_dim_customer.sql
│   │   └── ... (19 files total)
│   ├── 02_load_gold_batch1.sql
│   └── 03_load_gold_incremental.sql
├── create_v2_workflow.py          # Combined workflow (both batch + incremental)
├── create_v2_workflow_batch.py    # Batch-only workflow
├── create_v2_workflow_incremental.py  # Incremental-only workflow
└── WORKFLOW_README.md             # Detailed documentation
```

## Step 1: Create Batch Workflow

```bash
cd v2/databricks

python create_v2_workflow_batch.py \
  --workspace-path "/Workspace/Repos/your-org/your-repo/v2/databricks" \
  --job-name "TPC-DI-v2-Batch" \
  --output batch_workflow.json \
  --databricks-host "https://workspace.cloud.databricks.com" \
  --databricks-token "dapi..." \
  --create-job
```

This creates a workflow with:
- Setup task
- 17 Bronze table creation tasks
- 17 Silver table creation tasks  
- 19 Gold table creation tasks
- Bronze Batch 1 load
- Silver Batch 1 transform
- Gold Batch 1 load

## Step 2: Create Incremental Workflow

```bash
python create_v2_workflow_incremental.py \
  --workspace-path "/Workspace/Repos/your-org/your-repo/v2/databricks" \
  --job-name "TPC-DI-v2-Incremental" \
  --output incremental_workflow.json \
  --databricks-host "https://workspace.cloud.databricks.com" \
  --databricks-token "dapi..." \
  --create-job
```

This creates a workflow with:
- Setup task
- Bronze incremental load
- Silver incremental transform
- Gold incremental load

(Table creation tasks are skipped - tables already exist from Batch 1)

## Step 3: Run Batch 1

```bash
databricks jobs run-now \
  --job-id <batch-job-id> \
  --notebook-params '{
    "raw_data_path": "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10",
    "catalog": "tpcdi_catalog",
    "bronze_schema": "bronze_schema",
    "silver_schema": "silver_schema",
    "gold_schema": "gold_schema",
    "batch_id": "1",
    "warehouse_id": "your-warehouse-id"
  }'
```

## Step 4: Run Incremental (Batch 2+)

```bash
databricks jobs run-now \
  --job-id <incremental-job-id> \
  --notebook-params '{
    "raw_data_path": "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10",
    "catalog": "tpcdi_catalog",
    "bronze_schema": "bronze_schema",
    "silver_schema": "silver_schema",
    "gold_schema": "gold_schema",
    "batch_id": "2",
    "warehouse_id": "your-warehouse-id"
  }'
```

## Workflow Parameters

All workflows use these parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `raw_data_path` | `/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10` | TPC-DI data path |
| `catalog` | `tpcdi_catalog` | Unity Catalog name |
| `bronze_schema` | `bronze_schema` | Bronze schema |
| `silver_schema` | `silver_schema` | Silver schema |
| `gold_schema` | `gold_schema` | Gold schema |
| `batch_id` | `1` (batch) or `2` (incremental) | Batch number |
| `warehouse_id` | `` | SQL Warehouse ID (required) |

## Task Execution Order

### Batch Workflow
```
00_setup
  ├── bronze_create_* (17 tasks, parallel)
  │   └── bronze_load_batch1
  │       └── silver_create_* (17 tasks, parallel)
  │           └── silver_transform_batch1
  │               └── gold_create_* (19 tasks, parallel)
  │                   └── gold_load_batch1
```

### Incremental Workflow
```
00_setup
  └── bronze_load_incremental
      └── silver_transform_incremental
          └── gold_load_incremental
```

## Key Features

1. **Individual Table Files**: Each table has its own creation file for better organization
2. **Variable-Based**: All catalog/schema names use `${var.*}` syntax
3. **Separate Workflows**: Batch and Incremental are separate for clarity
4. **Parallel Table Creation**: Table creation tasks within a layer can run in parallel
5. **Proper Dependencies**: Tasks depend on previous layer completion

## Verification

After running, verify data:

```sql
-- Check Bronze
SELECT COUNT(*) FROM bronze_customer_mgmt WHERE _batch_id = 1;

-- Check Silver
SELECT COUNT(*) FROM silver_customers WHERE batch_id = 1;

-- Check Gold
SELECT COUNT(*) FROM gold_dim_customer;
SELECT COUNT(*) FROM gold_fact_trade;
```

## Troubleshooting

- **Warehouse ID Required**: Set `warehouse_id` parameter to a valid SQL Warehouse ID
- **File Not Found**: Verify `workspace_path` matches your repo structure
- **Table Already Exists**: This is OK - tables use `CREATE TABLE IF NOT EXISTS`
- **Variable Not Set**: Ensure setup task runs first to set all variables

See `WORKFLOW_README.md` for detailed troubleshooting.
