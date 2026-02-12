# TPC-DI Benchmark

[TPC-DI](https://www.tpc.org/tpcdi/) benchmark implementations for Data Integration: generate raw data and run ETL to a data warehouse.

## Project layout

- **`v1/`** – Original implementation: Python ETL, unified runner for Databricks/Dataproc/local, data generation script, and workflow/notebook helpers. See [v1/README.md](v1/README.md).
- **`v2/`** – SQL-based pipeline (Databricks): bronze/silver/gold SQL scripts, batch and incremental runs. See [v2/README.md](v2/README.md).
- **`tools/datagen/`** – Shared TPC-DI data generator (DIGen); used by v1 and v2.
- **`docs/`** – Schema and architecture notes.

## Overview

- **TPC-DI** models extracting, transforming, and loading data from OLTP and other sources into a data warehouse.
- **v1** provides: data generation (`generate_tpcdi_data.py`), Python ETL (`benchmark/`), and runners for Databricks, Dataproc, and local.
- **v2** provides: SQL-only ETL on Databricks with batch and incremental loads.

## Prerequisites

1. **Java 7+** – Available on standard Databricks runtimes.
2. **TPC-DI Tools v1.1.0** – You must download the official tools and place them in `tools/datagen/`.

### Download TPC-DI Tools

1. Go to [TPC-DI Tools v1.1.0](https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY) (or the [TPC specs page](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) → TPC-DI → Source Code).
2. Download **TPC-DI Tools v1.1.0** (e.g. `TPCDI_Tools_v1.1.0.zip`).
3. Unzip and copy into **`tools/datagen/`**:
   - `DIGen.jar`
   - The **`PDGF`** directory, **renamed to lowercase** `pdgf` (required on Linux/Databricks).

Your layout should look like:

```
tools/datagen/
├── DIGen.jar
├── pdgf/          # lowercase
└── README.txt     # (optional; this repo provides one)
```

See `tools/datagen/README.txt` for more detail.

## Quick Start

### Data generation (v1)

From the project root (tools/datagen must contain DIGen; see Prerequisites):

```bash
# Default: scale factor 10, output to dbfs:/mnt/tpcdi
python v1/generate_tpcdi_data.py

# Custom scale factor and output
python v1/generate_tpcdi_data.py -s 100 -o dbfs:/mnt/tpcdi

# Use a Unity Catalog Volume
python v1/generate_tpcdi_data.py -s 10 --use-volume --catalog tpcdi
```

### v1 benchmark (Databricks / Dataproc / local)

```bash
# From project root
python v1/run_benchmark.py databricks --load-type batch --scale-factor 10 --target-catalog main ...
python v1/run_benchmark.py dataproc --cluster my-cluster --load-type batch --scale-factor 10 ...
python v1/run_benchmark.py local --load-type batch --scale-factor 10 ...
```

See [v1/README.md](v1/README.md) and [v2/README.md](v2/README.md) for full usage.

## Scale Factors

| Scale factor | Approx. raw size | Notes |
|-------------|------------------|--------|
| 10         | ~1 GB            | Default; good for quick runs |
| 100        | ~10 GB           | |
| 1000       | ~97 GB           | Use a larger driver |
| 10000      | ~970 GB          | Use a storage-optimized driver |

Generation runs on the **driver** only. For large scale factors (e.g. &gt; 1000), use a driver with sufficient memory and local storage (`/local_disk0` on Databricks).

## Output Paths

- **DBFS**: `dbfs:/mnt/tpcdi` (or your mount). Data is written under `dbfs:/mnt/tpcdi/sf=<scale_factor>/`.
- **Unity Catalog Volume**: Use `--use-volume` and `--catalog`. The script creates `tpcdi_raw_data.tpcdi_volume` if missing and writes under `.../tpcdi_volume/sf=<scale_factor>/`.
- **Local**: Pass a local path (e.g. `/tmp/tpcdi`) when not using DBFS/Volume. Output is under `.../sf=<scale_factor>/`.

## Project layout (detailed)

```
tpcdi_benchmark/
├── README.md
├── v1/                             # Original Python ETL + runners
│   ├── README.md
│   ├── run_benchmark.py            # Unified runner (databricks / dataproc / local)
│   ├── run_benchmark_databricks.py
│   ├── run_benchmark_dataproc.py
│   ├── generate_tpcdi_data.py     # Data generation CLI
│   ├── benchmark/                 # Python ETL (bronze/silver/gold)
│   ├── databricks/                # v1 Databricks notebooks & workflow
│   ├── dataproc/                  # v1 Dataproc scripts
│   └── scripts/
├── v2/                             # SQL-based Databricks pipeline
│   └── ...
├── tools/
│   └── datagen/                   # DIGen (shared)
│       ├── README.txt
│       ├── DIGen.jar               # You add this
│       └── pdgf/                   # You add this (lowercase)
├── docs/
└── requirements.txt
```

## Requirements

- Python 3.8+
- Java 7+ (for DIGen)
- On Databricks: **pyspark** and **dbutils** (provided by the runtime)

No extra Python dependencies are required for the script itself. For local runs without Databricks, only the standard library is used when not writing to DBFS/Volumes.

## References

- [TPC-DI Benchmark](https://www.tpc.org/tpcdi/)
- [TPC-DI Specification (v1.1.0)](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) (PDF)
- [TPC-DI Tools Download](https://www.tpc.org/TPC_Documents_Current_Versions/download_programs/tools-download-request5.asp?bm_type=TPC-DI&bm_vers=1.1.0&mode=CURRENT-ONLY)
- [Databricks TPC-DI](https://github.com/shannon-barrow/databricks-tpc-di) – Full Databricks ETL implementation (notebooks, DLT, etc.)

## License

This wrapper code is provided as-is. The TPC-DI benchmark, specification, and DIGen tool are © TPC; use and distribution of the TPC tools are subject to the [TPC EULA](https://www.tpc.org/tpc_documents_current_versions/txt/EULA_v2.2.0.txt) and [Fair Use](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf) policies.
