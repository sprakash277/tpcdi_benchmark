# What Happens If You Don't Attach a Dataproc Metastore

When you run the TPC-DI benchmark on **Dataproc** without attaching a **Dataproc Metastore** (Google Cloud’s managed Hive Metastore Service), Spark uses the **default metastore and warehouse** on the cluster. Here’s what that means.

---

## Default behavior (no Dataproc Metastore attached)

1. **Metastore**  
   Spark uses the **embedded/default metastore** for the driver:
   - Usually **Derby** (embedded, single JVM) or a **local Hive** metastore on the driver node.
   - It stores database and table **metadata** (names, schemas, locations) only for that driver session.

2. **Warehouse**  
   Table data written by `saveAsTable()` goes to Spark’s **default warehouse**:
   - On Dataproc this is often **local HDFS** or **local disk** on the cluster (e.g. `/user/hive/warehouse` or similar), **not** GCS, unless you set `spark.sql.warehouse.dir` to a GCS path.

3. **During the job**  
   - `CREATE DATABASE IF NOT EXISTS tpcdi_warehouse` and `saveAsTable(...)` **succeed**.
   - Tables and row counts are visible for the duration of that Spark session.

4. **After the job or cluster ends**  
   - **Metastore:** Metadata is **ephemeral**. When the driver (or cluster) is gone, the default metastore is gone too, so database/table **definitions** are lost.
   - **Data:** If the warehouse was local/HDFS, the **data** is also lost when the cluster is deleted. If you had pointed the warehouse to GCS, the files would remain in GCS but Spark would no longer have metastore entries pointing at them, so you couldn’t query them as “tables” without re-creating the metastore or re-registering tables.

So **without** a Dataproc Metastore:

- The benchmark **runs and completes** (create database, create tables, ETL, metrics).
- Results are **not persistent** in a shared, stable way: metadata (and often data) are tied to the cluster/session and disappear when the job or cluster is gone.
- You **cannot** reliably reuse the same “database” or “tables” from another job or cluster, or after restarting the cluster.

---

## When to attach a Dataproc Metastore

Attach a **Dataproc Metastore** (and typically a GCS warehouse) if you want:

- **Persistent** database and table metadata across jobs and cluster restarts.
- **Shared** metadata so multiple clusters or jobs can see the same tables.
- Table **data** stored in **GCS** (by setting the Hive warehouse dir to `gs://...`), so it survives cluster deletion.

Then:

- Create a **Dataproc Metastore** service and attach it to the cluster (e.g. `--metastore-service` when creating the cluster).
- Configure Spark/Hive to use a **GCS warehouse** (e.g. `spark.sql.warehouse.dir=gs://your-bucket/warehouse` or equivalent Hive config).

The benchmark does **not** set the metastore or warehouse for you; it uses whatever Spark is configured with. So:

- **No metastore attached** → default (ephemeral) metastore and usually local warehouse; benchmark runs, but nothing is persistent.
- **Dataproc Metastore + GCS warehouse** → persistent metadata and data in GCS; you can reuse the same database/tables across runs and clusters.

---

## Summary

| Scenario | Metastore | Warehouse | After job / cluster gone |
|----------|-----------|-----------|---------------------------|
| **No Dataproc Metastore** | Default (e.g. Derby / local Hive) | Default (often local/HDFS) | Metadata and often data are **lost**; benchmark run is effectively one-off. |
| **Dataproc Metastore + GCS warehouse** | Managed Hive (persistent) | GCS | **Persistent**; same database/tables can be reused by other jobs/clusters. |

So: **if you don’t attach a Dataproc metastore, the benchmark still runs, but database and table metadata (and usually the table data) do not persist after the job or cluster is gone.**
