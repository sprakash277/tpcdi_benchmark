# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-DI Data Generation (Databricks)
# MAGIC
# MAGIC Generates TPC-DI benchmark raw data per the [TPC-DI v1.1.0 spec](https://www.tpc.org/tpcdi/).
# MAGIC Uses the official DIGen (Java) tool. **Prerequisites:** TPC-DI Tools v1.1.0 in `tools/datagen/` (DIGen.jar, pdgf/).
# MAGIC
# MAGIC - **Scale factor** controls data size (e.g. 10 ≈ 1GB, 100 ≈ 10GB).
# MAGIC - Output can go to **DBFS** or a **Unity Catalog Volume**.
# MAGIC - Generation runs on the **driver**; use a driver with sufficient local storage for large scale factors.
# MAGIC - **Upload threads** controls parallel file upload speed to DBFS (more threads = faster uploads).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widgets (optional)

# COMMAND ----------

try:
    dbutils.widgets.drop("scale_factor")
except Exception:
    pass
try:
    dbutils.widgets.drop("output_path")
except Exception:
    pass
try:
    dbutils.widgets.drop("raw_output_path")
except Exception:
    pass
try:
    dbutils.widgets.drop("tpcdi_raw_data_path")
except Exception:
    pass
try:
    dbutils.widgets.drop("upload_threads")
except Exception:
    pass

# Create widgets with defaults (for interactive use)
# When run as workflow task, these will be overridden by workflow parameters
dbutils.widgets.text("scale_factor", "10", "Scale factor (e.g. 10 ~ 1GB)")
dbutils.widgets.text("tpcdi_raw_data_path", "dbfs:/mnt/tpcdi", "TPC-DI raw data path: dbfs:/... (DBFS), /Volumes/... (Volume), gs://... (GCS), or local")
dbutils.widgets.text("upload_threads", "8", "Upload threads for DBFS (parallel file uploads)")

# COMMAND ----------

# Get parameters (from widgets or workflow parameters)
# Workflow parameters override widget defaults
# Support both tpcdi_raw_data_path (workflow parameter) and raw_output_path (backward compatibility)
scale_factor = int(dbutils.widgets.get("scale_factor"))
try:
    tpcdi_raw_data_path = dbutils.widgets.get("tpcdi_raw_data_path").strip()
except Exception:
    # Fallback to raw_output_path for backward compatibility
    try:
        tpcdi_raw_data_path = dbutils.widgets.get("raw_output_path").strip()
    except Exception:
        tpcdi_raw_data_path = "dbfs:/mnt/tpcdi"
raw_output_path = tpcdi_raw_data_path  # Use for generate_tpcdi_data() call
upload_threads = int(dbutils.widgets.get("upload_threads").strip() or "8")

print(f"Data Generation Parameters:")
print(f"  Scale Factor: {scale_factor}")
print(f"  TPC-DI Raw Data Path: {tpcdi_raw_data_path} (dbfs=DBFS, /Volumes/=Volume, gs://=GCS)")
print(f"  Upload Threads: {upload_threads}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run generation

# COMMAND ----------

import os
import sys
import importlib
from pathlib import Path

# Ensure project root is on path: derive from notebook path (parent of current notebook)
try:
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    workspace_path = str(Path(notebook_path).parent)
except Exception:
    workspace_path = os.getcwd()
sys.path.insert(0, str(Path(workspace_path).resolve()))

# Import and reload to pick up any code changes
import generate_tpcdi_data
importlib.reload(generate_tpcdi_data)
from generate_tpcdi_data import generate_tpcdi_data

out = generate_tpcdi_data(
    scale_factor=scale_factor,
    raw_output_path=raw_output_path,
    digen_path=None,
    skip_if_exists=True,
    upload_threads=upload_threads,
)
print("Output location:", out)

# COMMAND ----------
