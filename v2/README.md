# TPC-DI v2: run_tpcdi_batch + sql/

SQL-based TPC-DI pipeline on Databricks: one notebook (**run_tpcdi_batch**) runs Bronze → Silver → Gold using SQL under **sql/** and a few Python sub-notebooks.

## Structure

```
v2/
├── databricks/
│   ├── create_v2_workflow_notebook.py   # Creates Databricks job → run_tpcdi_batch
│   ├── run_tpcdi_batch.py               # Main notebook (Bronze → Silver → Gold)
│   ├── tpcdi_metrics.py                 # Metrics/reporting
│   └── sql/
│       ├── bronze/                      # Bronze SQL + batch/ (customer_mgmt, finwire .py)
│       ├── silver/                      # Silver SQL + batch/ (customers, accounts .py)
│       └── gold/                        # Gold SQL (load, incremental, optimize)
├── run_v2_databricks.py                 # Helper: print execution guide
└── RUN_V2.md                            # Full run instructions
```

## Usage (Databricks)

1. **Create job**: Open **databricks/create_v2_workflow_notebook.py**, set widgets, run → creates a job that runs **run_tpcdi_batch**.
2. **Run pipeline**: Trigger the job (or open **run_tpcdi_batch** and run manually with widgets set).

All SQL lives under **sql/bronze/**, **sql/silver/**, **sql/gold/**. Two Python steps use **sql/bronze/batch/** and **sql/silver/batch/** (CustomerMgmt/FinWire, customers/accounts).

## Batch vs incremental

- **load_type = batch**: Full load (Batch 1); run_tpcdi_batch runs bronze/silver/gold load SQL and sub-notebooks.
- **load_type = incremental**: Incremental (Batch 2+); run_tpcdi_batch runs sql/bronze/incremental/, sql/silver/incremental/, sql/gold/incremental/ and gold optimize.

## See also

- **RUN_V2.md** – Step-by-step run guide
- **databricks/QUICK_START.md**, **databricks/WORKFLOW_README.md** – Workflow and quick start
