# TPC-DI Benchmark: Databricks vs Dataproc

This directory contains a benchmarking framework to compare ETL performance between **Databricks** and **Dataproc** using the TPC-DI benchmark.

## Overview

The benchmark framework:
- Supports both **batch** and **incremental** ETL loads
- Works with **Databricks** (DBFS storage) and **Dataproc** (GCS storage)
- Collects detailed performance metrics
- Provides platform-agnostic ETL transformations

## Architecture

```
benchmark/
├── config.py          # Configuration management
├── metrics.py          # Performance metrics collection
├── runner.py           # Main benchmark orchestrator
├── platforms/          # Platform adapters
│   ├── databricks.py  # Databricks/DBFS adapter
│   └── dataproc.py    # Dataproc/GCS adapter
└── etl/               # ETL transformations
    ├── batch.py       # Batch load logic
    └── incremental.py # Incremental load logic
```

## Prerequisites

### Databricks
1. Databricks workspace with cluster
2. TPC-DI raw data (generated using `generate_tpcdi_data.py`)
3. **Output path** = raw data location (benchmark input):
   - **DBFS**: `dbfs:/mnt/tpcdi` → data at `dbfs:/mnt/tpcdi/sf=<scale_factor>/`
   - **Volume**: `/Volumes/<catalog>/<schema>/<volume>` → data at `.../volume/sf=<scale_factor>/`

### Dataproc
1. Dataproc cluster with GCS connector installed
2. TPC-DI raw data in GCS bucket
3. Data path: `gs://<bucket>/tpcdi/sf=<scale_factor>/`
4. GCP project ID and region

## Usage

### Databricks

#### Option 1: Databricks Notebook
1. Open `benchmark_databricks_notebook.py` in Databricks
2. Configure widgets:
   - **Load Type**: batch or incremental
   - **Scale Factor**: e.g., 10, 100, 1000
   - **Output Path**: Raw data location (DBFS or Volume base, e.g. `dbfs:/mnt/tpcdi`)
   - **Use Volume**: `true` if raw data is in a Unity Catalog Volume
   - **Target Database/Schema**: Where to write results
   - **Batch ID**: Required for incremental loads
   - **Metrics Output**: Where to save metrics JSON
3. Run all cells

#### Option 2: Python Script
```bash
# DBFS
python run_benchmark_databricks.py \
  --load-type batch \
  --scale-factor 10 \
  --output-path dbfs:/mnt/tpcdi \
  --target-database tpcdi_warehouse \
  --target-schema dw \
  --metrics-output dbfs:/mnt/tpcdi/metrics

# Unity Catalog Volume
python run_benchmark_databricks.py \
  --load-type batch \
  --scale-factor 10 \
  --output-path /Volumes/tpcdi/tpcdi_raw_data/tpcdi_volume \
  --use-volume \
  --target-database tpcdi_warehouse \
  --target-schema dw
```

For incremental:
```bash
python run_benchmark_databricks.py \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --output-path dbfs:/mnt/tpcdi \
  --target-database tpcdi_warehouse \
  --target-schema dw
```

### Dataproc

#### Submit as Spark Job
```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  -- \
  --load-type batch \
  --scale-factor 10 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1 \
  --target-database tpcdi_warehouse \
  --target-schema dw
```

For incremental:
```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=<cluster-name> \
  --region=us-central1 \
  -- \
  --load-type incremental \
  --scale-factor 10 \
  --batch-id 2 \
  --gcs-bucket=<your-bucket> \
  --project-id=<your-project> \
  --region=us-central1
```

## Load Types

### Batch Load
Processes historical data and initial batch:
- Loads dimension tables from `HistoricalLoad/` directory
- Loads fact and dimension tables from `Batch1/`
- Creates target database and schema
- Suitable for initial data warehouse setup

### Incremental Load
Processes a specific incremental batch:
- Updates dimension tables (SCD Type 2)
- Loads new fact records
- Requires `batch_id` parameter
- Processes files from `Batch<batch_id>/` directory

## Performance Metrics

The benchmark collects detailed metrics:

- **Step-level metrics**: Duration, rows processed, bytes processed per step
- **Summary metrics**: Total duration, throughput (rows/sec, MB/sec)
- **Table metrics**: Row counts and sizes for each table
- **Error tracking**: Failed steps with error messages

Metrics are saved as JSON files with timestamps:
- Databricks: `dbfs:/mnt/tpcdi/metrics/metrics_databricks_batch_sf10_20240126_143022.json`
- Dataproc: `gs://bucket/tpcdi/metrics/metrics_dataproc_batch_sf10_20240126_143022.json`

## Comparing Results

To compare Databricks vs Dataproc:

1. Run benchmark on both platforms with the same scale factor
2. Collect metrics JSON files
3. Compare key metrics:
   - Total duration
   - Throughput (rows/sec, MB/sec)
   - Per-step durations
   - Resource utilization (if available)

Example comparison script:
```python
import json

# Load metrics
with open("metrics_databricks_batch_sf10_*.json") as f:
    db_metrics = json.load(f)
with open("metrics_dataproc_batch_sf10_*.json") as f:
    dp_metrics = json.load(f)

print(f"Databricks: {db_metrics['total_duration_seconds']:.2f}s")
print(f"Dataproc: {dp_metrics['total_duration_seconds']:.2f}s")
print(f"Speedup: {dp_metrics['total_duration_seconds'] / db_metrics['total_duration_seconds']:.2f}x")
```

## ETL Transformations

The benchmark implements core TPC-DI transformations:

### Dimension Tables (Historical)
- `DimDate` - Date dimension
- `DimTime` - Time dimension
- `DimTradeType` - Trade type lookup
- `DimStatusType` - Status type lookup
- `DimTaxRate` - Tax rate lookup
- `DimIndustry` - Industry classification

### Dimension Tables (Batch/Incremental)
- `DimAccount` - Customer accounts (SCD Type 2)
- `DimCustomer` - Customer information (SCD Type 2)

### Fact Tables
- `FactTrade` - Trade transactions

**Note**: This is a simplified implementation. The full TPC-DI specification includes more tables and complex transformations. Extend the ETL modules as needed.

## Extending the Benchmark

### Add New ETL Transformations

1. Add methods to `benchmark/etl/batch.py` or `benchmark/etl/incremental.py`
2. Call from `run_full_batch_load()` or `process_batch()`
3. Use platform adapter methods: `read_raw_file()`, `write_table()`

### Add New Metrics

1. Extend `StepMetrics` in `benchmark/metrics.py`
2. Add metadata to step finish calls
3. Update summary calculations if needed

### Support New Platforms

1. Create new adapter in `benchmark/platforms/`
2. Implement required methods (matching `DatabricksPlatform` interface)
3. Add platform enum to `config.py`
4. Update `runner.py` to handle new platform

## Troubleshooting

### Databricks
- Ensure raw data exists at specified DBFS path
- Check cluster has sufficient resources for scale factor
- Verify SparkSession is available

### Dataproc
- Ensure GCS connector is installed on cluster
- Verify GCS bucket permissions
- Check that raw data is accessible from cluster
- Verify project ID and region are correct

## References

- [TPC-DI Specification](https://www.tpc.org/tpcdi/)
- [Databricks TPC-DI Implementation](https://github.com/shannon-barrow/databricks-tpc-di)
- [TPC-DI Tools](https://www.tpc.org/tpc_documents_current_versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY)
