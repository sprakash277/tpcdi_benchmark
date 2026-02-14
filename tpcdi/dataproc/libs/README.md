# libs/

Pre-bundled JARs used by the TPC-DI benchmark when running on **Dataproc** (or other Spark runtimes).

| JAR | Purpose |
|-----|--------|
| `spark-xml_2.12-0.18.0.jar` | [spark-xml](https://github.com/databricks/spark-xml) – read/write XML (e.g. `CustomerMgmt.xml`). Scala 2.12, Spark 3.x. |
| `spark-xml_2.13-0.18.0.jar` | Same, for Scala 2.13 (e.g. Dataproc serverless default runtime). |

## Usage

The benchmark adds `spark-xml` via `spark.jars.packages` by default (Maven). If your cluster has no Maven access (e.g. air-gapped), pass the local JAR with `--jars` when submitting. Use the **2.12** JAR for Scala 2.12 runtimes, or the **2.13** JAR for Scala 2.13 (e.g. many Dataproc serverless runtimes):

```bash
# Scala 2.12 (managed cluster)
--jars=dataproc/libs/spark-xml_2.12-0.18.0.jar

# Scala 2.13 (e.g. serverless)
--jars=dataproc/libs/spark-xml_2.13-0.18.0.jar
```

Example:

```bash
gcloud dataproc jobs submit pyspark run_benchmark_dataproc.py \
  --cluster=... --region=... \
  --py-files=benchmark.zip \
  --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
  -- \
  --load-type batch --scale-factor 10 --gcs-bucket=... --project-id=...
```

Run from the project root so the `dataproc/libs/` path resolves. The JAR is uploaded with the job.

## Source

- `spark-xml_2.12-0.18.0.jar`: [Maven Central](https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar)
- `spark-xml_2.13-0.18.0.jar`: [Maven Central](https://repo1.maven.org/maven2/com/databricks/spark-xml_2.13/0.18.0/spark-xml_2.13-0.18.0.jar)
