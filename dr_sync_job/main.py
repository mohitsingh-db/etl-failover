# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./validator

# COMMAND ----------

# MAGIC %run ./sync_to_region

# COMMAND ----------

config = load_config("./config/config_dev.json")
validated_config = validate_config(config)

# COMMAND ----------

validate_workflow_and_tables(validated_config, "secondary")
  

# COMMAND ----------

# Run sync using the secondary sync location with detailed debugging information
sync_to_region(
    validated_config,        # The validated configuration dictionary containing workspace and sync details
    "secondary",             # Use the 'secondary' sync location for this sync operation
    sync_interval_hours=10,  # Recheck groups that haven't been synced for at least 10 hours
    max_workers=10,          # Use up to 10 threads for parallel processing of workflows
    logging_level="debug"    # Set logging level to 'debug' for detailed logging output during sync
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from humana.main.workflow_1_gold

# COMMAND ----------

df = spark.read.format("delta").load("s3://one-env-uc-external-location/mohit/central/sync/meta/secondary_sync/humana/main/workflow_1_gold")
display(df)

# COMMAND ----------

display(spark.read.format("delta").load("s3://one-env-uc-external-location/mohit/central/sync/meta/secondary_sync/meta/sync_metadata_table"))