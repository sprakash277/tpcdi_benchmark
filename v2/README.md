# TPC-DI v2: run_tpcdi_batch + sql/

SQL-based TPC-DI pipeline: one runner (**run_tpcdi_batch**) runs Bronze → Silver → Gold using SQL under **sql/** and a few Python steps (CustomerMgmt/FinWire, silver customers/accounts). Same pattern on **Databricks** (notebook) and **Dataproc** (Delta, script).

## Structure

```
v2/
├── databricks/              # Databricks (Unity Catalog, run_tpcdi_batch notebook)
│   ├── create_v2_workflow_notebook.py
│   ├── run_tpcdi_batch.py
│   ├── tpcdi_metrics.py
│   └── sql/                 # bronze/, silver/, gold/
├── dataproc/                # Dataproc (Delta, Hive database, run_tpcdi_batch.py script)
│   ├── run_tpcdi_batch.py
│   ├── tpcdi_metrics.py
│   ├── run_dataproc_job.sh
│   └── sql/                 # bronze/ (FROM _tmp_*), silver/, gold/ (same logic, __DATABASE__)
├── run_v2_databricks.py
└── RUN_V2.md
```

## Usage (Databricks)

1. **Create job**: Open **databricks/create_v2_workflow_notebook.py**, set widgets, run → creates a job that runs **run_tpcdi_batch**.
2. **Run pipeline**: Trigger the job (or open **run_tpcdi_batch** and run manually with widgets set).

All SQL lives under **sql/bronze/**, **sql/silver/**, **sql/gold/**. Two Python steps use **sql/bronze/batch/** and **sql/silver/batch/** (CustomerMgmt/FinWire, customers/accounts).

## Batch vs incremental

- **load_type = batch**: Full load (Batch 1); run_tpcdi_batch runs bronze/silver/gold load SQL and sub-notebooks.
- **load_type = incremental**: Incremental (Batch 2+); run_tpcdi_batch runs sql/bronze/incremental/, sql/silver/incremental/, sql/gold/incremental/ and gold optimize.

## Dataproc (Delta)

Use **dataproc/run_tpcdi_batch.py** with `spark-submit` or `gcloud dataproc jobs submit pyspark`. Requires Delta Lake JAR and spark-xml JAR. See **dataproc/README.md** and **dataproc/run_dataproc_job.sh**.

## See also

- **RUN_V2.md** – Step-by-step run guide (Databricks)
- **databricks/QUICK_START.md**, **databricks/WORKFLOW_README.md** – Workflow and quick start
- **dataproc/README.md** – Dataproc (Delta) run guide
