# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Create Catalog and Schema
# MAGIC
# MAGIC This notebook creates the Unity Catalog and schema for the TPC-DI v2 implementation.
# MAGIC
# MAGIC **Parameters:**
# MAGIC - `catalog`: Unity Catalog name
# MAGIC - `schema_name`: Schema name (used for all layers: bronze, silver, gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "tpcdi_catalog", "Unity Catalog Name")
dbutils.widgets.text("schema_name", "tpcdi_schema", "Schema Name")

catalog = dbutils.widgets.get("catalog")
schema_name = dbutils.widgets.get("schema_name")

print(f"Verifying catalog: {catalog}")
print(f"Creating schema: {schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Catalog Exists

# COMMAND ----------

# Check if catalog exists
catalogs = spark.sql("SHOW CATALOGS").collect()
catalog_exists = any(row.catalog == catalog for row in catalogs)

if not catalog_exists:
    error_msg = f"ERROR: Catalog '{catalog}' does not exist. Please create it before running this workflow."
    print(f"❌ {error_msg}")
    raise ValueError(error_msg)

print(f"✓ Catalog '{catalog}' exists")

# Use catalog
spark.sql(f"USE CATALOG {catalog}")
print(f"✓ Using catalog '{catalog}'")

# MAGIC %md
# MAGIC ## Create Schema

# COMMAND ----------

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
print(f"✓ Schema '{schema_name}' created or already exists")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

# Verify catalog exists
catalogs = spark.sql("SHOW CATALOGS").collect()
catalog_exists = any(row.catalog == catalog for row in catalogs)
print(f"Catalog '{catalog}' exists: {catalog_exists}")

# Verify schema exists
spark.sql(f"USE CATALOG {catalog}")
schemas = spark.sql("SHOW SCHEMAS").collect()
schema_exists = any(row.databaseName == schema_name for row in schemas)
print(f"Schema '{schema_name}' exists: {schema_exists}")

if catalog_exists and schema_exists:
    print("\n✅ Setup completed successfully!")
else:
    print("\n⚠️  Setup completed with warnings. Please verify catalog and schema creation.")
