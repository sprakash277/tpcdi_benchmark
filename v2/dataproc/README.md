# TPC-DI v2 on Dataproc (Delta)

Same pattern as **v2/databricks**: one runner (**run_tpcdi_batch.py**) runs Bronze → Silver → Gold using SQL under **sql/** and Python for CustomerMgmt/FinWire and silver customers/accounts. All tables are **Delta**.

## Structure

```
v2/dataproc/
├── run_tpcdi_batch.py      # Main runner (spark-submit / gcloud dataproc jobs submit pyspark)
├── tpcdi_metrics.py        # Stats and benchmark report (Dataproc/Delta)
├── run_dataproc_job.sh     # Example: submit job to Dataproc
├── libs/                   # JARs: spark-xml (see libs/README.md); add spark-xml_2.12-0.18.0.jar
├── sql/
│   ├── bronze/             # Bronze load SQL (FROM _tmp_*; runner creates temp views)
│   │   ├── batch/          # load_bronze_customer_mgmt.py, load_bronze_finwire.py
│   │   └── incremental/
│   ├── silver/             # Same as v2/databricks (__CATALOG__.__SCHEMA__ → database)
│   └── gold/               # Same as v2/databricks
└── README.md
```

## Differences from v2/databricks

- **Catalog**: Single Hive **database** (e.g. `tpcdi_dw`) instead of Unity Catalog `catalog.schema`.
- **Placeholders**: SQL uses `__DATABASE__` and `__BATCH_ID__`; runner replaces with `--database` and `--batch-id`.
- **Bronze load**: Databricks uses `read_files()`; on Dataproc the runner creates temp views from `spark.read.text(path)` and SQL uses `FROM _tmp_bronze_*`.
- **Silver/Gold**: Same SQL as Databricks; runner replaces `__CATALOG__.__SCHEMA__` with database and `split_part(..., '|', n)` with `element_at(split(..., '\\|'), n)` for Spark SQL.
- **Delta**: Use Delta Lake JAR and Spark config for Delta.

## Prerequisites

1. **Delta Lake JAR** on the cluster (e.g. `gs://spark-lib/delta/delta-core_2.12-2.4.0.jar` or `--packages io.delta:delta-core_2.12:2.4.0`).
2. **spark-xml JAR** for CustomerMgmt.xml: add `spark-xml_2.12-0.18.0.jar` to **libs/** (see **libs/README.md**) or set `SPARK_XML_JAR` to a GCS path.
3. TPC-DI raw data in GCS (e.g. `gs://bucket/tpcdi/sf=10/Batch1/`).

## Run

### Option 1: run_dataproc_job.sh

```bash
cd v2/dataproc
export CLUSTER=my-cluster REGION=us-central1 PROJECT=my-project
export RAW_DATA_PATH=gs://my-bucket/tpcdi
./run_dataproc_job.sh
```

### Option 2: gcloud directly

```bash
cd v2/dataproc
zip -q tpcdi_metrics.zip tpcdi_metrics.py

gcloud dataproc jobs submit pyspark run_tpcdi_batch.py \
  --cluster=my-cluster --region=us-central1 --project=my-project \
  --py-files=tpcdi_metrics.zip \
  --jars=gs://spark-lib/delta/delta-core_2.12-2.4.0.jar,libs/spark-xml_2.12-0.18.0.jar \
  --properties=spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  -- \
  --database tpcdi_dw \
  --raw-data-path gs://my-bucket/tpcdi \
  --sf 10 \
  --load-type batch \
  --batch-id 1
```

### Option 3: spark-submit (on cluster node)

```bash
spark-submit \
  --packages io.delta:delta-core_2.12:2.4.0,com.databricks:spark-xml_2.12:0.18.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  run_tpcdi_batch.py \
  --database tpcdi_dw --raw-data-path gs://bucket/tpcdi --sf 10 --load-type batch --batch-id 1
```

## Parameters

| Argument           | Description |
|--------------------|-------------|
| --database         | Hive database name (default: tpcdi_dw) |
| --raw-data-path    | Base path to TPC-DI data (e.g. gs://bucket/tpcdi) |
| --sf               | Scale factor (default: 10) |
| --batch-id         | Batch ID (1 for batch, 2+ for incremental) |
| --load-type        | `batch` or `incremental` |
| --sql-base-path    | Base dir for sql/ (default: script dir) |
| --xml-format       | XML reader for CustomerMgmt (default: com.databricks.spark.xml) |

## Silver customers/accounts

**sql/silver/batch/transform_silver_customers.py** and **transform_silver_accounts.py** are included; they parse **bronze_customer_mgmt** (from CustomerMgmt.xml) and write **silver_customers** and **silver_accounts** (required for gold_dim_customer and gold_dim_account).
