"""
Cost estimation for TPC-DI v2 runs on Dataproc (GCP).
Copied from v1 benchmark.cost; use for comparison only (list-price approximations).
"""

from typing import Any, Dict, Optional

DATAPROC_FEE_PER_VCPU_HOUR = 0.01
DATAPROC_VCPU_HOUR: Dict[str, float] = {
    "n2d-standard-4": 4,
    "n2d-standard-8": 8,
    "n2d-standard-16": 16,
    "n2d-standard-32": 32,
    "n2d-standard-48": 48,
    "n2d-standard-64": 64,
    "n2d-highmem-4": 4,
    "n2d-highmem-8": 8,
    "n2d-highmem-16": 16,
    "n2d-highmem-32": 32,
    "n4-standard-4": 4,
    "n4-standard-8": 8,
    "n4-standard-16": 16,
    "n4-standard-32": 32,
    "e2-standard-4": 4,
    "e2-standard-8": 8,
    "e2-standard-16": 16,
}
DATAPROC_GB_PER_INSTANCE: Dict[str, float] = {
    "n2d-standard-4": 16,
    "n2d-standard-8": 32,
    "n2d-standard-16": 64,
    "n2d-standard-32": 128,
    "n2d-standard-48": 192,
    "n2d-standard-64": 256,
    "n2d-highmem-4": 32,
    "n2d-highmem-8": 64,
    "n2d-highmem-16": 128,
    "n2d-highmem-32": 256,
    "n4-standard-4": 16,
    "n4-standard-8": 32,
    "n4-standard-16": 64,
    "n4-standard-32": 128,
    "e2-standard-4": 16,
    "e2-standard-8": 32,
    "e2-standard-16": 64,
}
DATAPROC_USD_PER_VCPU_HOUR = 0.033
DATAPROC_USD_PER_GB_HOUR = 0.004
DATAPROC_DEFAULT_VCPU_PER_NODE = 16
DATAPROC_DEFAULT_GB_PER_NODE = 64


def _hours(seconds: Optional[float]) -> float:
    if seconds is None or seconds <= 0:
        return 0.0
    return seconds / 3600.0


def estimate_dataproc_cost(
    total_duration_seconds: Optional[float],
    cluster_worker_count: Optional[int],
    cluster_instance_type: Optional[str],
    cluster_master_type: Optional[str],
) -> Dict[str, Any]:
    """
    Estimate Dataproc job cost from duration and cluster metadata.
    Returns dict with keys: compute_usd, software_usd, total_usd, duration_hours.
    Uses default vCPU/GB per node when instance type is unknown.
    """
    duration_hours = _hours(total_duration_seconds)
    workers = cluster_worker_count if cluster_worker_count is not None and cluster_worker_count >= 0 else 0
    driver_vcpu = DATAPROC_VCPU_HOUR.get(
        (cluster_master_type or cluster_instance_type or "").strip(),
        DATAPROC_DEFAULT_VCPU_PER_NODE,
    )
    driver_gb = DATAPROC_GB_PER_INSTANCE.get(
        (cluster_master_type or cluster_instance_type or "").strip(),
        DATAPROC_DEFAULT_GB_PER_NODE,
    )
    worker_vcpu = DATAPROC_VCPU_HOUR.get(
        (cluster_instance_type or "").strip(),
        DATAPROC_DEFAULT_VCPU_PER_NODE,
    )
    worker_gb = DATAPROC_GB_PER_INSTANCE.get(
        (cluster_instance_type or "").strip(),
        DATAPROC_DEFAULT_GB_PER_NODE,
    )
    total_vcpu_hours = duration_hours * (driver_vcpu + workers * worker_vcpu)
    total_gb_hours = duration_hours * (driver_gb + workers * worker_gb)
    compute_usd = round(
        total_vcpu_hours * DATAPROC_USD_PER_VCPU_HOUR
        + total_gb_hours * DATAPROC_USD_PER_GB_HOUR,
        4,
    )
    software_usd = round(total_vcpu_hours * DATAPROC_FEE_PER_VCPU_HOUR, 4)
    total_usd = round(compute_usd + software_usd, 4)
    return {
        "compute_usd": compute_usd,
        "software_usd": software_usd,
        "total_usd": total_usd,
        "duration_hours": round(duration_hours, 4),
    }
