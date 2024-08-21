# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import expr, current_timestamp, col, lit

# Define paths and database
storageaccname = dbutils.widgets.get("storageaccname")
gold_table_path = f"abfss://external@{storageaccname}.dfs.core.windows.net/data/main/workflow_1_gold"  
database_name = "dr_test_catalog.main" 
table_name = "workflow_1_gold"  

# Create or replace the Gold table as an external Delta table and register it in Unity Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
        file_name STRING,
        parsed_data STRING,
        parsed_time TIMESTAMP,
        workspace_name STRING,
        merge_counter BIGINT,
        merge_time TIMESTAMP
    )
    USING DELTA
    LOCATION '{gold_table_path}'
""")

# Load the Silver table data
silver_df = spark.read.table("dr_test_catalog.stage.workflow_1_silver")

# Calculate the current max value of merge_counter from the Gold table
max_counter = spark.sql(f"SELECT COALESCE(MAX(merge_counter), 0) AS max_counter FROM {database_name}.{table_name}").collect()[0]['max_counter']

# Add a new merge counter starting from the current max counter
silver_df = silver_df.withColumn("merge_counter", expr(f"{max_counter} + monotonically_increasing_id() + 1"))
silver_df = silver_df.withColumn("merge_time", current_timestamp())

# Merge the Silver data into the Gold table
gold_table = DeltaTable.forPath(spark, gold_table_path)

gold_table.alias("tgt").merge(
    silver_df.alias("src"),
    "tgt.file_name = src.file_name"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

# Verify the creation and merging
spark.sql(f"SELECT * FROM {database_name}.{table_name}").show()
