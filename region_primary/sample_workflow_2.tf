resource "databricks_notebook" "generate_raw_data" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../sample_workflow_2/GenerateRawData.py"))
  path           = "/Workspace/sample_workflow_2/GenerateRawData"
  format         = "SOURCE"
  language       = "PYTHON"
}

resource "databricks_notebook" "transform_to_silver" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../sample_workflow_2/TransformToSilver.py"))
  path           = "/Workspace/sample_workflow_2/TransformToSilver"
  format         = "SOURCE"
  language       = "PYTHON"
}

resource "databricks_notebook" "aggregate_to_gold" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../sample_workflow_2/AggregateToGold.py"))
  path           = "/Workspace/sample_workflow_2/AggregateToGold"
  format         = "SOURCE"
  language       = "PYTHON"
}

resource "databricks_job" "workflow" {
  provider = databricks.workspace
  name = "sample_workflow_2"

  task {
    task_key       = "GenerateRawData"
    notebook_task {
      notebook_path = databricks_notebook.generate_raw_data.path
    }
  }

  task {
    task_key       = "TransformToSilver"
    depends_on {
      task_key = "GenerateRawData"
    }
    notebook_task {
      notebook_path = databricks_notebook.transform_to_silver.path
    }
  }

  task {
    task_key       = "AggregateToGold"
    depends_on {
      task_key = "TransformToSilver"
    }
    notebook_task {
      notebook_path = databricks_notebook.aggregate_to_gold.path
    }
  }

  schedule {
    quartz_cron_expression = "0 0/5 * * * ?"  # Cron schedule for every 5 minutes
    timezone_id = "UTC"
    
    # Pause/Unpause setting: 'UNPAUSED' = active, 'PAUSED' = paused
    pause_status = "UNPAUSED"  # Set to "PAUSED" to initially pause the job
  }
}
