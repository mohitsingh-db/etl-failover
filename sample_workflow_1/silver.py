# Databricks notebook source
from pyspark.sql.functions import from_unixtime, col
from pyspark.sql.types import StructType, StructField, StringType

# Read data from the bronze table
bronze_df = spark.read.table("dr_test_catalog.raw.workflow_1_bronze")

# Define the schema for the parsed data
schema = StructType([
    StructField("parsed_data", StringType(), True)
])

# Parse the binary content
silver_df = (bronze_df.withColumn("parsed_data", bronze_df["content"].cast("string"))  # Cast content to string as an example of parsing
             .withColumn("parsed_time", from_unixtime(col("file_modified_time").cast("long")))  # Convert file_modified_time to human-readable time
             .select("file_name", "parsed_data", "parsed_time", "workspace_name"))

# Save the parsed data to a Silver table
silver_df.write.format("delta").mode("overwrite").saveAsTable("dr_test_catalog.stage.workflow_1_silver")

# sleep for 2 mins
import time
time.sleep(120)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dr_test_catalog.stage.workflow_1_silver