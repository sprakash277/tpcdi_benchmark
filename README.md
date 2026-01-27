# TPC-DI Data Generation for Databricks

Generate [TPC-DI](https://www.tpc.org/tpcdi/) benchmark raw data using Python on **Databricks**, per the TPC-DI v1.1.0 specification. Uses the official **DIGen** (Java) data generator invoked via a Python wrapper.

## Overview

- **TPC-DI** is a benchmark for Data Integration: it models extracting, transforming, and loading data from OLTP and other sources into a data warehouse.
- This project provides:
  - **`generate_tpcdi_data.py`** – CLI / library script that runs DIGen and uploads output to DBFS or a Unity Catalog Volume.
  - **`generate_tpcdi_data_notebook.py`** – Databricks notebook with widgets for scale factor, output path, and Volume vs DBFS.

Data generation runs on the **Databricks driver** (single node). Output can be written to **DBFS** or a **Unity Catalog Volume**.

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

### 1. Run via Python (CLI)

From the project root (e.g. on a Databricks job or locally with Java + tools installed):

```bash
# Default: scale factor 10, output to dbfs:/mnt/tpcdi
python generate_tpcdi_data.py

# Custom scale factor and output
python generate_tpcdi_data.py -s 100 -o dbfs:/mnt/tpcdi

# Use a Unity Catalog Volume
python generate_tpcdi_data.py -s 10 --use-volume --catalog tpcdi

# Regenerate even if output exists
python generate_tpcdi_data.py -s 10 -o dbfs:/mnt/tpcdi --no-skip-existing
```

### 2. Run via Databricks Notebook

1. Clone or upload this repo into **Databricks Repos** (or place the files in a workspace folder).
2. Open **`generate_tpcdi_data_notebook.py`** as a Databricks notebook.
3. Ensure **`tools/datagen/`** (with DIGen.jar and pdgf/) is in the same repo/folder.
4. Adjust widgets: **Scale factor**, **Output path**, **Use Unity Catalog Volume**, **Catalog**.
5. Run all cells.

The notebook calls `generate_tpcdi_data()` and prints the output path.

### 3. Run as a Databricks Job

1. Create a Job with a **Python script** task.
2. Set the **Source** to the repo path of **`generate_tpcdi_data.py`** (or upload the script).
3. Set **Parameters** if needed, e.g. `-s 10 -o dbfs:/mnt/tpcdi --use-volume`.
4. Use a **cluster** with a driver that has enough **local disk** for the chosen scale factor (see below).

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

## Project Layout

```
tpcdi_benchmark/
├── README.md
├── generate_tpcdi_data.py          # Main script
├── generate_tpcdi_data_notebook.py # Databricks notebook
├── tools/
│   └── datagen/
│       ├── README.txt              # Setup instructions
│       ├── DIGen.jar               # You add this
│       └── pdgf/                   # You add this (lowercase)
└── requirements.txt                # Optional
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
