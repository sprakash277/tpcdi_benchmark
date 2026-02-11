# TPC-DI v2 Workflow Guide

## Overview

This guide explains how to create and run Databricks workflows for the v2 SQL-only implementation.

## Quick Start

### Option 1: Using Databricks Notebook (Recommended)

1. **Open the workflow creation notebook**:
   - Navigate to `v2/databricks/create_v2_workflow_notebook.py` in Databricks
   - Or import it as a notebook

2. **Configure widgets**:
   - Set `workspace_path` to your SQL files location (e.g., `/Workspace/Repos/org/repo/v2/databricks`)
   - Set `warehouse_id` (required - get from SQL Warehouse settings)
   - Configure catalog, schemas, data paths
   - Choose `workflow_type`: `batch` or `incremental`
   - Set cluster configuration

3. **Run all cells**:
   - The notebook will create the workflow automatically
   - Copy the Job ID from the output

4. **Run the workflow**:
   - Via UI: Workflows → Jobs → Select job → Run now
   - Via CLI: `databricks jobs run-now --job-id <job-id>`

### Option 2: Using Python Script

```bash
python v2/databricks/create_v2_workflow.py \
  --workspace-path "/Workspace/Repos/org/repo/v2/databricks" \
  --warehouse-id "your-warehouse-id" \
  --job-name "TPC-DI-v2-SQL" \
  --workflow-type batch \
  --catalog tpcdi_catalog \
  --bronze-schema bronze_schema \
  --silver-schema silver_schema \
  --gold-schema gold_schema \
  --raw-data-path "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10" \
  --output v2_workflow.json

# Create workflow
databricks jobs create --json-file v2_workflow.json
```

## Workflow Structure

The workflow consists of:

### 1. Setup Task
- Creates catalog and schemas
- Runs first (no dependencies)

### 2. Bronze Layer Tasks
- **Table Creation**: Individual tasks for each Bronze table (14 tables)
  - `bronze_create_bronze_customer_mgmt`
  - `bronze_create_bronze_customer`
  - `bronze_create_bronze_account`
  - ... (all Bronze tables)
- **Load Tasks**:
  - Batch: `bronze_load_batch1` (depends on all table creation tasks)
  - Incremental: `bronze_load_incremental` (depends on all table creation tasks)

### 3. Silver Layer Tasks
- **Table Creation**: Individual tasks for each Silver table (17 tables)
  - `silver_create_silver_customers`
  - `silver_create_silver_accounts`
  - ... (all Silver tables)
- **Transform Tasks**:
  - Batch: `silver_transform_batch1` (depends on Bronze load + Silver table creation)
  - Incremental: `silver_transform_incremental` (depends on Bronze load + Silver table creation)

### 4. Gold Layer Tasks
- **Table Creation**: Individual tasks for each Gold table (18 tables)
  - `gold_create_gold_dim_customer`
  - `gold_create_gold_dim_account`
  - ... (all Gold tables)
- **Load Tasks**:
  - Batch: `gold_load_batch1` (depends on Silver transform + Gold table creation)
  - Incremental: `gold_load_incremental` (depends on Silver transform + Gold table creation)

## Task Dependencies

```
00_setup
  ├─ bronze_create_* (14 tasks, parallel)
  │   └─ bronze_load_batch1 (batch) OR bronze_load_incremental (incremental)
  │
  ├─ silver_create_* (17 tasks, parallel)
  │   └─ silver_transform_batch1 (batch) OR silver_transform_incremental (incremental)
  │       (depends on bronze_load_*)
  │
  └─ gold_create_* (18 tasks, parallel)
      └─ gold_load_batch1 (batch) OR gold_load_incremental (incremental)
          (depends on silver_transform_*)
```

## Batch vs Incremental

### Batch Workflow (Load Type: `batch`)
- Creates all tables (if not exists)
- Loads Batch 1 data (historical)
- Uses `INSERT OVERWRITE` for Silver transforms
- Uses `INSERT` for Gold loads

### Incremental Workflow (Load Type: `incremental`)
- Tables must already exist (created in Batch 1)
- Loads incremental data (Batch 2+)
- Uses `MERGE` for Silver transforms (SCD Type 2)
- Uses `MERGE` for Gold loads (upsert)

**Important**: For incremental loads, set `batch_id` parameter (e.g., `2`, `3`, etc.)

## Workflow Parameters

The workflow accepts these parameters (can be overridden at runtime):

| Parameter | Default | Description |
|-----------|---------|-------------|
| `load_type` | `batch` | `batch` or `incremental` |
| `batch_id` | `1` | Batch ID (for incremental loads) |
| `catalog` | `tpcdi_catalog` | Unity Catalog name |
| `bronze_schema` | `bronze_schema` | Bronze schema name |
| `silver_schema` | `silver_schema` | Silver schema name |
| `gold_schema` | `gold_schema` | Gold schema name |
| `raw_data_path` | `/Volumes/...` | Path to TPC-DI raw data |

## Running Workflows

### Via Databricks UI

1. Go to **Workflows** → **Jobs**
2. Find your job (e.g., "TPC-DI-v2-SQL")
3. Click **Run now**
4. Override parameters if needed:
   - `load_type`: `batch` or `incremental`
   - `batch_id`: `2` (for incremental)
   - `raw_data_path`: Your data path
5. Click **Run**

### Via Databricks CLI

```bash
# Batch load
databricks jobs run-now \
  --job-id <job-id> \
  --notebook-params '{
    "load_type": "batch",
    "raw_data_path": "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10"
  }'

# Incremental load (Batch 2)
databricks jobs run-now \
  --job-id <job-id> \
  --notebook-params '{
    "load_type": "incremental",
    "batch_id": "2",
    "raw_data_path": "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10"
  }'
```

### Via Databricks API

```bash
curl -X POST \
  https://your-workspace.cloud.databricks.com/api/2.1/jobs/run-now \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": <job-id>,
    "notebook_params": {
      "load_type": "batch",
      "raw_data_path": "/Volumes/tpcdi_catalog/tpcdi_schema/tpcdi_volume/sf=10"
    }
  }'
```

## Monitoring Workflow Execution

### View Task Status

1. Go to **Workflows** → **Jobs** → Select job → View runs
2. Click on a run to see task execution status
3. Each task shows:
   - Status (Success/Failed/Running)
   - Duration
   - Logs

### Collect Metrics

After workflow completes, run the metrics collection notebook:

1. Open `v2/databricks/collect_metrics_notebook.py`
2. Set widgets:
   - Catalog and schema names
   - Platform, compute type, load type
   - Scale factor, batch ID
3. Run all cells
4. Review formatted report (similar to v1 output)

## Troubleshooting

### Workflow Fails at Table Creation

- **Error**: Table already exists
  - **Solution**: Tables are created with `IF NOT EXISTS`, so this shouldn't happen. Check for schema/catalog issues.

- **Error**: Permission denied
  - **Solution**: Ensure service principal/user has CREATE TABLE permissions on catalog/schema

### Workflow Fails at Data Load

- **Error**: File not found
  - **Solution**: Verify `raw_data_path` parameter points to correct location
  - Check that Batch files exist (Batch1/, Batch2/, etc.)

- **Error**: Variable not found
  - **Solution**: Ensure SQL files use `${var.xxx}` syntax correctly
  - Check that parameters are passed to SQL tasks

### Workflow Fails at Transform

- **Error**: Table not found
  - **Solution**: Ensure Bronze tables were created and loaded successfully
  - Check task dependencies are correct

- **Error**: MERGE conflict
  - **Solution**: For incremental loads, ensure Batch 1 completed successfully
  - Check for duplicate keys in source data

## Best Practices

1. **Use SQL Warehouse**: Set `warehouse_id` for better performance and cost control
2. **Monitor Task Duration**: Check individual task durations to identify bottlenecks
3. **Use Existing Clusters**: For repeated runs, configure `existing_cluster_id` to avoid startup time
4. **Parameterize Everything**: Use workflow parameters instead of hardcoding values
5. **Collect Metrics**: Run metrics collection after each workflow run to track performance

## File Structure

```
v2/databricks/
├── create_v2_workflow.py              # Python script to generate workflow JSON
├── create_v2_workflow_notebook.py     # Databricks notebook (recommended)
├── collect_metrics.py                 # Python script for metrics collection
├── collect_metrics_notebook.py        # Databricks notebook for metrics
├── bronze/
│   ├── tables/                        # Individual table creation files
│   │   ├── create_bronze_customer_mgmt.sql
│   │   ├── create_bronze_customer.sql
│   │   └── ... (14 files)
│   ├── 01_create_bronze_tables.sql   # Original combined file (for reference)
│   ├── 02_load_bronze_batch1.sql
│   └── 03_load_bronze_incremental.sql
├── silver/
│   ├── tables/                        # Individual table creation files
│   │   ├── create_silver_customers.sql
│   │   └── ... (17 files)
│   ├── 01_create_silver_tables.sql
│   ├── 02_transform_silver_batch1.sql
│   └── 03_transform_silver_incremental.sql
└── gold/
    ├── tables/                        # Individual table creation files
    │   ├── create_gold_dim_customer.sql
    │   └── ... (18 files)
    ├── 01_create_gold_tables.sql
    ├── 02_load_gold_batch1.sql
    └── 03_load_gold_incremental.sql
```

## Next Steps

1. **Create Workflow**: Use the notebook or script to create your workflow
2. **Run Batch 1**: Execute batch workflow to load historical data
3. **Run Incremental**: Execute incremental workflow for Batch 2+
4. **Collect Metrics**: Run metrics collection notebook after each run
5. **Optimize**: Review metrics and optimize slow tasks
