# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, expr, col

workspace_name = "e2-demo-field-eng"  # Replace with the actual workspace name
storageaccname = dbutils.widgets.get("storageaccname")

# Read the binary files from a directory
binary_df = (spark.read.format("binaryFile")
             .load(f"abfss://external@{storageaccname}.dfs.core.windows.net/data/ingestion/")
.withColumn("file_modified_time", col("modificationTime")).withColumn("file_name", expr("_metadata.file_path"))  # Adjusted line
             .withColumn("workspace_name", lit(workspace_name)))

# Save the binary data, filename, modified time, and workspace name to a Delta table
binary_df.write.format("delta").mode("overwrite").saveAsTable("dr_test_catalog.raw.workflow_1_bronze")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from dr_test_catalog.raw.workflow_1_bronze