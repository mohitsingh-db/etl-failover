# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./validator

# COMMAND ----------

# MAGIC %run ./sync_to_region

# COMMAND ----------

LOG_LEVEL = "debug"  # Default log level
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
    max_workers=10          # Use up to 10 threads for parallel processing of workflows
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from humana.main.workflow_1_gold