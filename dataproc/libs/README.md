# libs/

Pre-bundled JARs used by the TPC-DI benchmark when running on **Dataproc** (or other Spark runtimes).

| JAR | Purpose |
|-----|--------|
| `spark-xml_2.12-0.18.0.jar` | [spark-xml](https://github.com/databricks/spark-xml) – read/write XML (e.g. `CustomerMgmt.xml`). Scala 2.12, Spark 3.x. |

## Usage

The benchmark adds `spark-xml` via `spark.jars.packages` by default (Maven). If your cluster has no Maven access (e.g. air-gapped), pass the local JAR with `--jars` when submitting:

```bash
gcloud dataproc jobs submit pyspark dataproc/run_benchmark_dataproc.py \
  --cluster=... --region=... \
  --py-files=benchmark.zip \
  --jars=dataproc/libs/spark-xml_2.12-0.18.0.jar \
  -- \
  --load-type batch --scale-factor 10 --gcs-bucket=... --project-id=...
```

Run from the project root so `dataproc/libs/spark-xml_2.12-0.18.0.jar` resolves. The JAR is uploaded with the job.

## Source

- `spark-xml_2.12-0.18.0.jar`: [Maven Central](https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar)
