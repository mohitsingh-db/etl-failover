# Databricks notebook source
# MAGIC %run ./utils

# COMMAND ----------

# MAGIC %run ./validator

# COMMAND ----------

# MAGIC %run ./sync_to_region

# COMMAND ----------

# MAGIC %run ./sync_in_region

# COMMAND ----------

# Set up widgets for input parameters
dbutils.widgets.text("operation_type", "sender")  # Operation: "sender" or "receiver"
dbutils.widgets.text("sync_location_to", "secondary")  # Sync location: "primary" or "secondary"

# Retrieve parameter values
operation_type = dbutils.widgets.get("operation_type")
sync_location_to = dbutils.widgets.get("sync_location_to")

# COMMAND ----------

# Explanation of Parameters:
# - operation_type: This determines whether the notebook will act as a sender or receiver.
#                   "sender" will call the sync_to_region function to send data from one region to another.
#                   "receiver" will call the sync_in_region function to sync data into the target region.
# - sync_location_to: This determines which sync location (primary or secondary) to use for reading and writing the data.

LOG_LEVEL = "debug"  # Default log level

# Load configuration from JSON file
config = load_config("./config/config_dev.json")
validated_config = validate_config(config)



# COMMAND ----------

# Based on the operation type, either "sync_to_region" or "sync_in_region" will be executed.

if operation_type == "sender":
    # Sender: Sync to region (deep clone from one location to another)
    # Validate workflows and tables before syncing
    validate_workflow_and_tables(validated_config, sync_location_to)    
    sync_to_region(
        validated_config,        # The validated configuration dictionary containing workspace and sync details
        sync_location_to=sync_location_to,      # Use the 'primary' or 'secondary' sync location
        max_workers=10           # Use up to 10 threads for parallel processing of workflows
    )
elif operation_type == "receiver":
    # Receiver: Sync data from the external location into Unity Catalog
    sync_in_region(
        validated_config,             # The validated configuration dictionary containing workspace and sync details
        sync_location_from=sync_location_to,  # Use the 'primary' or 'secondary' sync location to read from
        max_workers=10                # Use up to 10 threads for parallel processing
    )
else:
    log_message("error", f"Invalid operation_type: {operation_type}. Must be either 'sender' or 'receiver'.")
    raise ValueError(f"Invalid operation_type: {operation_type}. Must be either 'sender' or 'receiver'.")

# COMMAND ----------