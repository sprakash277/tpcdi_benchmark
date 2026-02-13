"""
Cost estimation for TPC-DI v2 runs on Databricks (and Dataproc).
Copied from v1 benchmark.cost; use for comparison only (list-price approximations).
"""

from typing import Any, Dict, Optional

# --- Databricks: DBU list price per cloud (USD per DBU, approximate list price)
DATABRICKS_DBU_PRICE_PER_DBU: Dict[str, float] = {
    "AWS": 0.15,
    "Azure": 0.14,
    "GCP": 0.15,
}

DATABRICKS_DBU_PER_NODE_HOUR = 4.0

DATABRICKS_INSTANCE_USD_PER_HOUR: Dict[str, Dict[str, float]] = {
    "AWS": {
        "i3.xlarge": 0.312,
        "i3.2xlarge": 0.624,
        "i3.4xlarge": 1.248,
        "m5d.xlarge": 0.272,
        "m5d.2xlarge": 0.544,
        "m5d.4xlarge": 1.088,
        "r5d.xlarge": 0.288,
        "r5d.2xlarge": 0.576,
        "r5d.4xlarge": 1.152,
    },
    "GCP": {
        "n2d-standard-4": 0.193,
        "n2d-standard-8": 0.386,
        "n2d-standard-16": 0.77,
        "n2d-standard-32": 1.54,
        "n2d-standard-48": 2.31,
        "n2d-standard-64": 3.08,
        "n2d-standard-80": 3.85,
        "n2d-standard-96": 4.62,
        "n2d-highmem-4": 0.242,
        "n2d-highmem-8": 0.484,
        "n2d-highmem-16": 0.968,
        "n2d-highmem-32": 1.936,
        "c2-standard-4": 0.208,
        "c2-standard-8": 0.416,
        "c2-standard-16": 0.832,
        "c2-standard-30": 1.56,
    },
    "AZURE": {
        "Standard_E4s_v3": 0.20,
        "Standard_E8s_v3": 0.40,
        "Standard_E16s_v3": 0.80,
        "Standard_E32s_v3": 1.60,
    },
}
DATABRICKS_DEFAULT_USD_PER_NODE_HOUR = 0.50

# --- Dataproc: GCP list prices (USD)
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


def _databricks_instance_usd_per_hour(cloud: str, instance_type: Optional[str]) -> float:
    if not instance_type:
        return DATABRICKS_DEFAULT_USD_PER_NODE_HOUR
    cloud_key = (cloud or "AWS").upper()
    prices = DATABRICKS_INSTANCE_USD_PER_HOUR.get(cloud_key, {})
    return prices.get((instance_type or "").strip(), DATABRICKS_DEFAULT_USD_PER_NODE_HOUR)


def estimate_databricks_cost(
    total_duration_seconds: Optional[float],
    cluster_worker_count: Optional[int],
    cluster_instance_type: Optional[str],
    cluster_master_type: Optional[str],
    databricks_compute_type: Optional[str],
    cloud: str,
) -> Dict[str, Any]:
    """
    Estimate Databricks job cost from duration and cluster metadata.
    Returns dict with keys: compute_usd, dbu_usd, total_usd, software_usd, dbu_consumed, duration_hours.
    """
    duration_hours = _hours(total_duration_seconds)
    cloud_key = (cloud or "AWS").upper()
    if cloud_key not in ("AWS", "AZURE", "GCP"):
        cloud_key = "AWS"
    price_per_dbu = DATABRICKS_DBU_PRICE_PER_DBU.get(cloud_key, DATABRICKS_DBU_PRICE_PER_DBU["AWS"])
    driver_price = _databricks_instance_usd_per_hour(cloud_key, cluster_master_type or cluster_instance_type)
    worker_price = _databricks_instance_usd_per_hour(cloud_key, cluster_instance_type)
    workers = cluster_worker_count if cluster_worker_count is not None and cluster_worker_count >= 0 else 0
    if (databricks_compute_type or "").lower() == "serverless" and cluster_worker_count is None:
        workers = 0
    compute_usd = round(duration_hours * (driver_price + workers * worker_price), 4)
    num_nodes = 1 + workers
    dbu_per_hour = num_nodes * DATABRICKS_DBU_PER_NODE_HOUR
    dbu_consumed = duration_hours * dbu_per_hour
    dbu_usd = round(dbu_consumed * price_per_dbu, 4)
    total_usd = round(compute_usd + dbu_usd, 4)
    return {
        "compute_usd": compute_usd,
        "dbu_usd": dbu_usd,
        "software_usd": dbu_usd,
        "total_usd": total_usd,
        "dbu_consumed": round(dbu_consumed, 2),
        "duration_hours": round(duration_hours, 4),
        "cloud": cloud_key,
    }


def estimate_dataproc_cost(
    total_duration_seconds: Optional[float],
    cluster_worker_count: Optional[int],
    cluster_instance_type: Optional[str],
    cluster_master_type: Optional[str],
) -> Dict[str, Any]:
    """
    Estimate Dataproc job cost from duration and cluster metadata.
    Returns dict with keys: compute_usd, software_usd, total_usd, duration_hours.
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
