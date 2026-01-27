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
    dbutils.widgets.drop("use_volume")
except Exception:
    pass
try:
    dbutils.widgets.drop("catalog")
except Exception:
    pass
try:
    dbutils.widgets.drop("schema")
except Exception:
    pass

dbutils.widgets.text("scale_factor", "10", "Scale factor (e.g. 10 ~ 1GB)")
dbutils.widgets.text("output_path", "dbfs:/mnt/tpcdi", "Output path (DBFS or base for Volume)")
dbutils.widgets.dropdown("use_volume", "false", ["true", "false"], "Use Unity Catalog Volume")
dbutils.widgets.text("catalog", "tpcdi", "Catalog (when use_volume=true)")
dbutils.widgets.text("schema", "tpcdi_raw_data", "Schema (when use_volume=true)")

# COMMAND ----------

scale_factor = int(dbutils.widgets.get("scale_factor"))
output_path = dbutils.widgets.get("output_path").strip()
use_volume = dbutils.widgets.get("use_volume") == "true"
catalog = dbutils.widgets.get("catalog").strip() or "tpcdi"
schema = dbutils.widgets.get("schema").strip() or "tpcdi_raw_data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run generation

# COMMAND ----------

import os
import sys
from pathlib import Path

# Ensure project root (workspace path) is on path so we can import generate_tpcdi_data
try:
    workspace_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().workspacePath().get()
except Exception:
    workspace_path = os.getcwd()
sys.path.insert(0, str(Path(workspace_path).resolve()))

from generate_tpcdi_data import generate_tpcdi_data

out = generate_tpcdi_data(
    scale_factor=scale_factor,
    output_path=output_path,
    digen_path=None,
    use_volume=use_volume,
    catalog=catalog,
    schema=schema,
    skip_if_exists=True,
)
print("Output location:", out)

# COMMAND ----------
