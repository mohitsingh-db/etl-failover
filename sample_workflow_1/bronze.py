# Databricks notebook source
from pyspark.sql.functions import current_timestamp, lit, expr, col

workspace_name = "e2-demo-field-eng"  # Replace with the actual workspace name

# Read the binary files from a directory
binary_df = (spark.read.format("binaryFile")
             .load("s3://one-env-uc-external-location/mohit/east/data/ingestion/")
.withColumn("file_modified_time", col("modificationTime")).withColumn("file_name", expr("_metadata.file_path"))  # Adjusted line
             .withColumn("workspace_name", lit(workspace_name)))

# Save the binary data, filename, modified time, and workspace name to a Delta table
binary_df.write.format("delta").mode("overwrite").saveAsTable("humana.raw.workflow_1_bronze")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from humana.raw.workflow_1_bronze