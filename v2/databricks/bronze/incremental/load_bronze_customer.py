# Databricks notebook source
# Incremental load bronze_customer (widgets set by orchestrator)
catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")
raw_data_path = dbutils.widgets.get("raw_data_path")
sf = dbutils.widgets.get("sf")
batch_id = int(dbutils.widgets.get("batch_id"))
full_raw_data_path = f"{raw_data_path}/sf={sf}"

spark.sql(f"""
INSERT INTO {catalog}.{schema_name}.bronze_customer (raw_line, _batch_id, _load_timestamp, _source_file)
SELECT 
    value AS raw_line,
    {batch_id} AS _batch_id,
    current_timestamp() AS _load_timestamp,
    'Customer.txt' AS _source_file
FROM read_files('{full_raw_data_path}/Batch{batch_id}/Customer.txt', format => 'text', lineSep => '\\n')
WHERE value IS NOT NULL AND value != ''
""")
