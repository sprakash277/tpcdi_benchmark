#!/usr/bin/env python3
"""
TPC-DI Data Generation for Databricks

Generates TPC-DI benchmark raw data per the TPC-DI v1.1.0 specification
(https://www.tpc.org/tpcdi/). Uses the official DIGen (Java) tool invoked
via a Python wrapper. Designed to run on Databricks (driver node).

Usage:
    # Databricks notebook: use widgets or set variables, then call main()
    # CLI (local or Databricks job):
    python generate_tpcdi_data.py --scale-factor 10 --output dbfs:/mnt/tpcdi/sf10

Prerequisites:
    - Java 7+ (available on Databricks runtime)
    - TPC-DI Tools v1.1.0 extracted in tools/datagen/ (DIGen.jar, pdgf/, etc.)
    - See tools/datagen/README.txt and README.md for setup.
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Optional Databricks imports (only when running on Databricks)
try:
    import dbutils  # type: ignore
    IN_DATABRICKS = True
except ImportError:
    IN_DATABRICKS = False
    dbutils = None

try:
    from pyspark.sql import SparkSession
    spark: Optional[SparkSession] = None
    if IN_DATABRICKS:
        spark = SparkSession.builder.getOrCreate()
except Exception:
    spark = None


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

DRIVER_ROOT = "/local_disk0"  # Databricks driver local disk
DEFAULT_SCALE_FACTOR = 10
DEFAULT_DIGEN_PATH = "tools/datagen"
TPCDI_TMP = "tpcdi_tmp"


def _repo_root() -> Path:
    """Infer repo root (script location or workspace)."""
    if IN_DATABRICKS and dbutils:
        try:
            ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
            ws = ctx.workspacePath().get()
            if ws:
                return Path(ws)
        except Exception:
            pass
        return Path.cwd()
    return Path(__file__).resolve().parent


def _default_digen_path() -> Path:
    return _repo_root() / DEFAULT_DIGEN_PATH


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _copy_directory(src: Path, dst: Path, overwrite: bool = True) -> None:
    if dst.exists() and overwrite:
        shutil.rmtree(dst)
    if not dst.exists():
        shutil.copytree(src, dst)


def _run_digen(digen_path: Path, scale_factor: int, output_path: Path) -> None:
    """Run DIGen.jar. DIGen may prompt for EULA; we send YES via stdin."""
    jar = digen_path / "DIGen.jar"
    if not jar.exists():
        raise FileNotFoundError(
            f"DIGen.jar not found at {jar}. "
            "Download TPC-DI Tools v1.1.0 and extract into tools/datagen/. See README.md."
        )
    pdgf = digen_path / "pdgf"
    if not pdgf.is_dir():
        raise FileNotFoundError(
            f"pdgf/ not found under {digen_path}. "
            "TPC-DI Tools must include the pdgf directory (use lowercase 'pdgf')."
        )

    output_path.mkdir(parents=True, exist_ok=True)
    cmd = f"java -jar {jar} -sf {scale_factor} -o {output_path}"
    args = shlex.split(cmd)
    proc = subprocess.Popen(
        args,
        cwd=str(digen_path),
        universal_newlines=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    proc.stdin.write("\n")
    proc.stdin.write("YES\n")
    proc.stdin.flush()
    proc.stdin.close()

    if proc.stdout:
        for line in iter(proc.stdout.readline, ""):
            line = line.rstrip()
            if line:
                print(line)
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(f"DIGen exited with code {proc.returncode}")


def _upload_to_dbfs(local_dir: Path, dbfs_path: str) -> None:
    """Copy generated files from local_dir to DBFS. Uses /dbfs FUSE mount on Databricks."""
    if not IN_DATABRICKS:
        raise RuntimeError("Upload to DBFS requires Databricks.")

    # Normalize: dbfs:/path -> /dbfs/path
    if dbfs_path.startswith("dbfs:"):
        os_path = "/dbfs" + dbfs_path[5:]
    elif dbfs_path.startswith("/dbfs"):
        os_path = dbfs_path
    else:
        os_path = "/dbfs/" + dbfs_path.lstrip("/")

    dest = Path(os_path)
    dest.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(local_dir, topdown=True):
        for d in dirs:
            (dest / os.path.relpath(os.path.join(root, d), local_dir)).mkdir(
                parents=True, exist_ok=True
            )
        for f in files:
            src = Path(root) / f
            rel = os.path.relpath(src, local_dir)
            (dest / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest / rel)


def _upload_to_volume(local_dir: Path, volume_path: str) -> None:
    """Copy generated files to a Unity Catalog volume. Volume path: /Volumes/catalog/schema/vol/."""
    # /Volumes/... is mounted under /Volumes/... on the driver
    if not volume_path.startswith("/Volumes/"):
        raise ValueError("Volume path must start with /Volumes/")
    dest = Path(volume_path)
    dest.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(local_dir, topdown=True):
        for d in dirs:
            (dest / os.path.relpath(os.path.join(root, d), local_dir)).mkdir(parents=True, exist_ok=True)
        for f in files:
            src = Path(root) / f
            rel = os.path.relpath(src, local_dir)
            (dest / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest / rel)


def _upload_local(local_dir: Path, output_path: str) -> None:
    """Copy to a local path (e.g. /tmp/out)."""
    dest = Path(output_path)
    dest.mkdir(parents=True, exist_ok=True)
    for root, dirs, files in os.walk(local_dir, topdown=True):
        for d in dirs:
            (dest / os.path.relpath(os.path.join(root, d), local_dir)).mkdir(parents=True, exist_ok=True)
        for f in files:
            src = Path(root) / f
            rel = os.path.relpath(src, local_dir)
            (dest / rel).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest / rel)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def generate_tpcdi_data(
    scale_factor: int = DEFAULT_SCALE_FACTOR,
    output_path: str = "dbfs:/mnt/tpcdi",
    digen_path: Optional[str] = None,
    use_volume: bool = False,
    catalog: Optional[str] = None,
    skip_if_exists: bool = True,
) -> str:
    """
    Generate TPC-DI raw data and optionally upload to DBFS or a UC Volume.

    Args:
        scale_factor: TPC-DI scale factor (e.g. 10 ~ 1GB, 100 ~ 10GB).
        output_path: DBFS path (dbfs:/...), UC Volume (/Volumes/...), or local dir.
        digen_path: Path to folder containing DIGen.jar and pdgf/. Default: tools/datagen.
        use_volume: If True, write to Unity Catalog Volume. Sets output_path to
                    /Volumes/<catalog>/tpcdi_raw_data/tpcdi_volume/sf=<sf>.
        catalog: Catalog name when use_volume=True. Default: tpcdi.
        skip_if_exists: If output already exists, skip generation.

    Returns:
        Final path where data was written (DBFS or Volume path).
    """
    root = _repo_root()
    digen = Path(digen_path) if digen_path else _default_digen_path()
    if not digen.is_absolute():
        digen = root / digen

    # Determine where to generate (use driver local disk on Databricks)
    if IN_DATABRICKS and Path(DRIVER_ROOT).exists():
        base_tmp = Path(DRIVER_ROOT) / "tmp" / TPCDI_TMP
    else:
        base_tmp = Path("/tmp") / TPCDI_TMP
    driver_tmp = base_tmp / "datagen"
    driver_out = base_tmp / f"sf={scale_factor}"

    # Copy DIGen tools to driver temp (DIGen expects to run from its directory)
    _ensure_dir(base_tmp)
    _copy_directory(digen, driver_tmp, overwrite=True)
    digen_run = driver_tmp  # run from copied location

    # Output destination
    if use_volume:
        cat = catalog or "tpcdi"
        volume_path = f"/Volumes/{cat}/tpcdi_raw_data/tpcdi_volume/sf={scale_factor}"
        if skip_if_exists and IN_DATABRICKS and spark is not None:
            try:
                existing = spark.sql(
                    f"SELECT 1 FROM system.information_schema.volumes "
                    f"WHERE catalog_name = '{cat}' AND schema_name = 'tpcdi_raw_data' AND name = 'tpcdi_volume'"
                ).first()
                if existing:
                    # Check sf folder
                    vol_full = f"/Volumes/{cat}/tpcdi_raw_data/tpcdi_volume/sf={scale_factor}"
                    if Path(vol_full).exists():
                        print(f"Volume path {vol_full} already exists; skipping generation.")
                        return vol_full
            except Exception:
                pass
        create_volume_if_needed(cat, spark)
        final_dest = volume_path
    else:
        final_dest = (output_path.rstrip("/") + f"/sf={scale_factor}").replace("//", "/")

    # Skip if exists
    if skip_if_exists:
        if final_dest.startswith("dbfs:"):
            check_path = "/dbfs" + final_dest[5:]
            if os.path.exists(check_path) and os.listdir(check_path):
                print(f"Output {final_dest} already exists; skipping generation.")
                return final_dest
        elif final_dest.startswith("/Volumes/"):
            if Path(final_dest).exists() and any(Path(final_dest).iterdir()):
                print(f"Output {final_dest} already exists; skipping generation.")
                return final_dest
        elif os.path.isdir(final_dest) and os.listdir(final_dest):
            print(f"Output {final_dest} already exists; skipping generation.")
            return final_dest

    print(f"Generating TPC-DI data (scale factor={scale_factor}) into {driver_out}")
    _run_digen(digen_run, scale_factor, driver_out)
    print("Generation complete. Uploading to destination...")

    if use_volume:
        _upload_to_volume(driver_out, final_dest)
    elif final_dest.startswith("dbfs:") or final_dest.startswith("/dbfs"):
        dbfs_arg = ("dbfs:" + final_dest[5:]) if final_dest.startswith("/dbfs") else final_dest
        _upload_to_dbfs(driver_out, dbfs_arg)
    else:
        _upload_local(driver_out, final_dest)

    print(f"Done. Data written to {final_dest}")
    return final_dest


def create_volume_if_needed(catalog: str, spark_session: Optional[SparkSession]) -> None:
    """Create catalog, schema, and volume if they do not exist."""
    if not IN_DATABRICKS or spark_session is None:
        return
    spark_session.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark_session.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog} TO `account users`")
    spark_session.sql(
        f"CREATE DATABASE IF NOT EXISTS {catalog}.tpcdi_raw_data "
        "COMMENT 'Schema for TPC-DI Raw Files Volume'"
    )
    spark_session.sql(
        f"CREATE VOLUME IF NOT EXISTS {catalog}.tpcdi_raw_data.tpcdi_volume "
        "COMMENT 'TPC-DI Raw Files'"
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Generate TPC-DI benchmark data (Databricks-ready).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    ap.add_argument(
        "-s", "--scale-factor",
        type=int,
        default=DEFAULT_SCALE_FACTOR,
        help="TPC-DI scale factor (e.g. 10 ~ 1GB)",
    )
    ap.add_argument(
        "-o", "--output",
        default="dbfs:/mnt/tpcdi",
        help="Output path: dbfs:/... or /Volumes/cat/schema/vol",
    )
    ap.add_argument(
        "-d", "--digen-path",
        default=None,
        help="Path to DIGen (DIGen.jar + pdgf/). Default: tools/datagen",
    )
    ap.add_argument(
        "--use-volume",
        action="store_true",
        help="Write to Unity Catalog Volume (tpcdi.tpcdi_raw_data.tpcdi_volume)",
    )
    ap.add_argument(
        "--catalog",
        default="tpcdi",
        help="Catalog name when --use-volume",
    )
    ap.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Always regenerate even if output exists",
    )
    args = ap.parse_args()

    generate_tpcdi_data(
        scale_factor=args.scale_factor,
        output_path=args.output,
        digen_path=args.digen_path,
        use_volume=args.use_volume,
        catalog=args.catalog,
        skip_if_exists=not args.no_skip_existing,
    )


if __name__ == "__main__":
    main()
    sys.exit(0)
