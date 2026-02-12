# libs/

JARs used by **v2/dataproc** (run_tpcdi_batch.py) when running on Dataproc.

| JAR | Purpose |
|-----|--------|
| `spark-xml_2.12-0.18.0.jar` | [spark-xml](https://github.com/databricks/spark-xml) – read XML (e.g. `CustomerMgmt.xml`). Scala 2.12, Spark 3.x. |

## Add the JAR

Download and place the JAR here so `run_dataproc_job.sh` can find it:

```bash
cd v2/dataproc/libs
curl -L -o spark-xml_2.12-0.18.0.jar \
  https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.18.0/spark-xml_2.12-0.18.0.jar
```

Or use a GCS path and set `SPARK_XML_JAR` when running:

```bash
export SPARK_XML_JAR=gs://your-bucket/jars/spark-xml_2.12-0.18.0.jar
./run_dataproc_job.sh
```

## Delta Lake

Delta is typically provided via GCS (e.g. `gs://spark-lib/delta/delta-core_2.12-2.4.0.jar`) or `--packages io.delta:delta-core_2.12:2.4.0`. See `run_dataproc_job.sh` and README.md.
