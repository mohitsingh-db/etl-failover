# Upload notebooks to the workspace as actual notebooks
resource "databricks_notebook" "file_checker" {
  content_base64 = base64encode(file("../sample_workflow_1/file_checker.py"))
  path           = "/Workspace/sample_workflow_1/file_checker"
  language       = "PYTHON"
  format         = "SOURCE"
  provider       = databricks.workspace
}

resource "databricks_notebook" "bronze" {
  content_base64 = base64encode(file("../sample_workflow_1/bronze.py"))
  path           = "/Workspace/sample_workflow_1/bronze"
  language       = "PYTHON"
  format         = "SOURCE"
  provider       = databricks.workspace
}

resource "databricks_notebook" "silver" {
  content_base64 = base64encode(file("../sample_workflow_1/silver.py"))
  path           = "/Workspace/sample_workflow_1/silver"
  language       = "PYTHON"
  format         = "SOURCE"
  provider       = databricks.workspace
}

resource "databricks_notebook" "archive" {
  content_base64 = base64encode(file("../sample_workflow_1/archive.py"))
  path           = "/Workspace/sample_workflow_1/archive"
  language       = "PYTHON"
  format         = "SOURCE"
  provider       = databricks.workspace
}

resource "databricks_notebook" "gold" {
  content_base64 = base64encode(file("../sample_workflow_1/gold.py"))
  path           = "/Workspace/sample_workflow_1/gold"
  language       = "PYTHON"
  format         = "SOURCE"
  provider       = databricks.workspace
}

# Create a Databricks Job (workflow) with multiple tasks using the all-purpose cluster
resource "databricks_job" "sample_workflow_1" {
  name = "sample_workflow_1"
  max_concurrent_runs = 1
  provider = databricks.workspace

  parameter {
    name   = "storageaccname"
    default = azurerm_storage_account.dr_test_centralus_storage.name
  }

  task {
    task_key = "file_checker"
    notebook_task {
      notebook_path = "/Workspace/sample_workflow_1/file_checker"
    }
  }

  task {
    task_key = "status"
    depends_on {
      task_key = "file_checker"
    }
    condition_task {
      op = "EQUAL_TO"
      left = "{{tasks.file_checker.values.file_check_status}}"
      right = "true"
    }
  }

  task {
    task_key = "adls_to_raw"
    depends_on {
      task_key = "status"
      outcome = "true"
    }
    notebook_task {
      notebook_path = "/Workspace/sample_workflow_1/bronze"
    }
  }

  task {
    task_key = "raw_to_stage"
    depends_on {
      task_key = "adls_to_raw"
    }
    notebook_task {
      notebook_path = "/Workspace/sample_workflow_1/silver"
    }
  }

  task {
    task_key = "archive"
    depends_on {
      task_key = "raw_to_stage"
    }
    notebook_task {
      notebook_path = "/Workspace/sample_workflow_1/archive"
    }
  }

  task {
    task_key = "stage_to_main"
    depends_on {
      task_key = "archive"
    }
    notebook_task {
      notebook_path = "/Workspace/sample_workflow_1/gold"
    }
  }

  schedule {
    quartz_cron_expression = "0 0/5 * * * ?"  # Cron schedule for every 5 minutes
    timezone_id = "UTC"
    
    # Pause/Unpause setting: 'UNPAUSED' = active, 'PAUSED' = paused
    pause_status = "PAUSED"  # Set to "PAUSED" to initially pause the job
  }
}
