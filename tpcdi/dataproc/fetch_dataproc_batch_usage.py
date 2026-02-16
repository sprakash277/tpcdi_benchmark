#!/usr/bin/env python3
"""
Post-run script: fetch Dataproc serverless batch usage and merge into metrics.

Call this AFTER a Dataproc batch job has completed (SUCCEEDED). The batch API
populates runtimeInfo.approximateUsage (milliDcuSeconds, shuffleStorageGbSeconds)
only after the batch finishes, so this script must run outside the batch (e.g.
from the same wrapper that submitted the batch and ran `gcloud dataproc batches wait`).

Usage:
  python fetch_dataproc_batch_usage.py --batch-id BATCH_ID --region REGION --project PROJECT [--metrics-output PATH]

  --metrics-output: Directory (gs://bucket/path or local) or path to a specific metrics JSON.
                    If directory: writes standalone usage JSON and merges into the latest metrics_dataproc_serverless_*.json.
                    If file: merges into that file.
                    If omitted: only writes standalone usage JSON to current directory.

Requires: gcloud in PATH (for batches describe and, when using GCS, gsutil for read/write).
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def run_cmd(cmd: list, capture: bool = True) -> tuple[int, str]:
    """Run command; return (returncode, stdout+stderr)."""
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture,
            text=True,
            timeout=120,
        )
        out = (result.stdout or "") + (result.stderr or "")
        return (result.returncode, out)
    except Exception as e:
        return (-1, str(e))


def describe_batch(project: str, region: str, batch_id: str) -> dict | None:
    """Get batch resource JSON via gcloud. batch_id can be short (e.g. ac596a404fa548a2a6e38f00aa41c0c8) or full name."""
    # Allow short id; gcloud accepts the last segment
    bid = batch_id.split("/")[-1] if "/" in batch_id else batch_id
    cmd = [
        "gcloud",
        "dataproc",
        "batches",
        "describe",
        bid,
        f"--region={region}",
        f"--project={project}",
        "--format=json",
    ]
    code, out = run_cmd(cmd)
    if code != 0:
        print(f"Error describing batch: {out}", file=sys.stderr)
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON from gcloud: {e}", file=sys.stderr)
        return None


def _parse_rfc3339(s: str | None) -> datetime | None:
    """Parse RFC3339 timestamp; return naive UTC datetime or None."""
    if not s:
        return None
    try:
        # Handle optional fractional seconds and Z
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
    except (ValueError, TypeError):
        return None


def build_usage_payload(full_batch: dict) -> dict:
    """Extract cost/usage and config for metrics merge (aligned with batch UI fields)."""
    create_time = full_batch.get("createTime")
    state_time = full_batch.get("stateTime")
    payload = {
        "batch_name": full_batch.get("name"),
        "batch_uuid": full_batch.get("uuid"),
        "state": full_batch.get("state"),
        "create_time": create_time,
        "state_time": state_time,
        "labels": full_batch.get("labels"),
        "creator": full_batch.get("creator"),
    }
    # Elapsed time (create -> state) in seconds
    t0, t1 = _parse_rfc3339(create_time), _parse_rfc3339(state_time)
    if t0 is not None and t1 is not None:
        payload["elapsed_seconds"] = max(0, (t1 - t0).total_seconds())

    usage = (full_batch.get("runtimeInfo") or {}).get("approximateUsage") or {}
    if usage:
        payload["approximate_usage"] = usage
        milli_dcu = usage.get("milliDcuSeconds") or 0
        shuffle_gb_sec = usage.get("shuffleStorageGbSeconds") or 0
        payload["approximate_dcu_hours"] = round(milli_dcu / 1_000_000 * (1 / 3600), 6)
        # GB-months: 1 GB-month ≈ 30 * 24 * 3600 GB-seconds
        payload["approximate_shuffle_storage_gb_months"] = round(
            shuffle_gb_sec / (30 * 24 * 3600), 6
        )

    rc = full_batch.get("runtimeConfig") or {}
    payload["runtime_config"] = {
        "version": rc.get("version"),
        "properties": rc.get("properties"),
    }
    if rc.get("acceleratorType") is not None:
        payload["runtime_config"]["accelerator_type"] = rc.get("acceleratorType")

    return payload


def is_gcs(path: str) -> bool:
    return path.startswith("gs://")


def gcs_list_json_files(gcs_dir: str) -> list[str]:
    """List *.json files under gs://bucket/path/ (no recursive). Returns full gs:// paths."""
    code, out = run_cmd(["gsutil", "ls", gcs_dir.rstrip("/") + "/*.json"])
    if code != 0:
        return []
    return [line.strip() for line in out.splitlines() if line.strip().endswith(".json")]


def gcs_read_json(gcs_path: str) -> dict | None:
    """Download GCS path and parse JSON."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        tmp = f.name
    try:
        code, _ = run_cmd(["gsutil", "-q", "cp", gcs_path, tmp])
        if code != 0:
            return None
        with open(tmp, "r") as f:
            return json.load(f)
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def gcs_write_json(gcs_path: str, data: dict) -> bool:
    """Write JSON to GCS path."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f, indent=2)
        tmp = f.name
    try:
        code, out = run_cmd(["gsutil", "-q", "cp", tmp, gcs_path])
        if code != 0:
            print(f"Failed to write {gcs_path}: {out}", file=sys.stderr)
            return False
        return True
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


# Merge only into serverless metrics files (this script runs after a serverless batch).
METRICS_SERVERLESS_PREFIX = "metrics_dataproc_serverless_"


def local_latest_metrics_file(local_dir: str) -> str | None:
    """Return path to the latest metrics_dataproc_serverless_*.json in directory (by mtime)."""
    p = Path(local_dir)
    if not p.is_dir():
        return None
    files = list(p.glob(f"{METRICS_SERVERLESS_PREFIX}*.json"))
    if not files:
        return None
    latest = max(files, key=lambda f: f.stat().st_mtime)
    return str(latest)


def gcs_latest_metrics_file(gcs_dir: str) -> str | None:
    """Return gs:// path to the latest metrics_dataproc_serverless_*.json (by name sort; filename has timestamp)."""
    files = gcs_list_json_files(gcs_dir)
    metrics_files = [f for f in files if f"/{METRICS_SERVERLESS_PREFIX}" in f and f.endswith(".json")]
    if not metrics_files:
        return None
    # Sort by name descending (timestamp in filename) and take first
    metrics_files.sort(reverse=True)
    return metrics_files[0]


def merge_into_metrics_file(metrics_path: str, dataproc_batch: dict, is_gcs_path: bool) -> bool:
    """Read metrics JSON, add dataproc_batch key, write back."""
    if is_gcs_path:
        data = gcs_read_json(metrics_path)
    else:
        try:
            with open(metrics_path, "r") as f:
                data = json.load(f)
        except Exception as e:
            print(f"Failed to read {metrics_path}: {e}", file=sys.stderr)
            return False
    if data is None:
        return False
    data["dataproc_batch"] = dataproc_batch
    if is_gcs_path:
        return gcs_write_json(metrics_path, data)
    try:
        with open(metrics_path, "w") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        print(f"Failed to write {metrics_path}: {e}", file=sys.stderr)
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch Dataproc batch usage and optionally merge into metrics JSON"
    )
    parser.add_argument("--batch-id", required=True, help="Dataproc batch ID (from submit output)")
    parser.add_argument("--region", required=True, help="GCP region (e.g. us-central1)")
    parser.add_argument("--project", required=True, help="GCP project ID")
    parser.add_argument(
        "--metrics-output",
        help="Metrics output directory (gs:// or local) or path to a specific metrics JSON file. If directory, merges into latest metrics_dataproc_serverless_*.json only.",
    )
    args = parser.parse_args()

    full = describe_batch(args.project, args.region, args.batch_id)
    if full is None:
        return 1

    payload = build_usage_payload(full)
    short_id = (args.batch_id.split("/")[-1] if "/" in args.batch_id else args.batch_id)[:16]
    standalone_name = f"dataproc_batch_{short_id}_usage.json"

    # Write standalone usage file
    if args.metrics_output:
        base = args.metrics_output.rstrip("/")
        if is_gcs(base):
            # gs://bucket/path or gs://bucket/path/file.json
            if base.endswith(".json"):
                standalone_path = base.replace(".json", f"_batch_usage_{short_id}.json")
            else:
                standalone_path = f"{base}/{standalone_name}"
        else:
            if base.endswith(".json"):
                standalone_path = base.replace(".json", f"_batch_usage_{short_id}.json")
            else:
                standalone_path = str(Path(base) / standalone_name)
    else:
        standalone_path = str(Path.cwd() / standalone_name)

    if is_gcs(standalone_path):
        if not gcs_write_json(standalone_path, payload):
            return 1
    else:
        Path(standalone_path).parent.mkdir(parents=True, exist_ok=True)
        with open(standalone_path, "w") as f:
            json.dump(payload, f, indent=2)
    print(f"Wrote batch usage to {standalone_path}")

    # Optionally merge into latest metrics file
    if args.metrics_output:
        base = args.metrics_output.rstrip("/")
        is_file = base.endswith(".json")
        if is_file:
            metrics_file = base
        else:
            if is_gcs(base):
                metrics_file = gcs_latest_metrics_file(base)
            else:
                metrics_file = local_latest_metrics_file(base)
        if metrics_file:
            if merge_into_metrics_file(metrics_file, payload, is_gcs(metrics_file)):
                print(f"Merged dataproc_batch into {metrics_file}")
            else:
                print(f"Could not merge into {metrics_file}", file=sys.stderr)
        else:
            print(f"No {METRICS_SERVERLESS_PREFIX}*.json found in {base}; standalone file only.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
