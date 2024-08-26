resource "databricks_notebook" "dr_sync_job_main" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../dr_sync_job/main.py"))
  path           = "/Workspace/dr_sync_job/main"
  language       = "PYTHON"
}

resource "databricks_notebook" "dr_sync_job_sync_to_region" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../dr_sync_job/sync_to_region.py"))
  path           = "/Workspace/dr_sync_job/sync_to_region"
  language       = "PYTHON"
}

resource "databricks_notebook" "dr_sync_job_sync_in_region" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../dr_sync_job/sync_in_region.py"))
  path           = "/Workspace/dr_sync_job/sync_in_region"
  language       = "PYTHON"
}

resource "databricks_notebook" "dr_sync_job_utils" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../dr_sync_job/utils.py"))
  path           = "/Workspace/dr_sync_job/utils"
  language       = "PYTHON"
}

resource "databricks_notebook" "dr_sync_job_validator" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../dr_sync_job/validator.py"))
  path           = "/Workspace/dr_sync_job/validator"
  language       = "PYTHON"
}

resource "databricks_workspace_file" "dr_sync_job_config" {
  provider = databricks.workspace  
  content_base64 = base64encode(file("../dr_sync_job/config/config_dev.json"))   
  path   = "/Workspace/dr_sync_job/config/config_dev.json"  
}


resource "databricks_job" "dr_receiver_job" {
  name = "dr_receiver"
  max_concurrent_runs = 1
  provider = databricks.workspace

  # Define the parameters for the job
  parameter {
    name    = "operation_type"
    default = "receiver"
  }

  parameter {
    name    = "sync_location_to"
    default = "primary"
  }

  # Define the first task
  task {
    task_key = "receive_data"
    
    # Notebook task configuration
    notebook_task {
      notebook_path = "/Workspace/dr_sync_job/main"
    }

    # No cluster specified here to use serverless by default
  }

  # Task schedule (runs every 5 minutes)
  schedule {
    quartz_cron_expression = "0 0/5 * * * ?"  # Cron schedule for every 5 minutes
    timezone_id = "UTC"
    
    # Pause/Unpause setting: 'UNPAUSED' = active, 'PAUSED' = paused
    pause_status = "PAUSED"  # Change this to "PAUSED" to pause the job initially
  }

  # Set email notifications
  email_notifications {
    no_alert_for_skipped_runs = false
  }
}

# To pause or unpause the job after it's been deployed, modify the `pause_status` field:
# pause_status = "PAUSED"   -> To pause the job
# pause_status = "UNPAUSED" -> To unpause the job

# Sender Job
resource "databricks_job" "dr_sender_job" {
  name = "dr_sender"
  max_concurrent_runs = 1
  provider = databricks.workspace

  # Define parameters for the sender job
  parameter {
    name    = "operation_type"
    default = "sender"
  }

  parameter {
    name    = "sync_location_to"
    default = "secondary"
  }

  # Task for sender job
  task {
    task_key = "send_data"
    
    # Notebook task configuration for sender
    notebook_task {
      notebook_path = "/Workspace/dr_sync_job/main"
    }

    # No cluster specified to use serverless by default
  }

  # Schedule to run every 5 minutes
  schedule {
    quartz_cron_expression = "0 0/5 * * * ?"  # Cron schedule for every 5 minutes
    timezone_id = "UTC"
    
    # Pause/Unpause setting: 'UNPAUSED' = active, 'PAUSED' = paused
    pause_status = "UNPAUSED"  # Set to "PAUSED" to initially pause the job
  }

  # Set email notifications
  email_notifications {
    no_alert_for_skipped_runs = false
  }
}