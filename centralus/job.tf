resource "databricks_notebook" "generate_raw_data" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../job_notebooks/GenerateRawData.py"))
  path           = "/Workspace/GenerateRawData"
  language       = "PYTHON"
}

resource "databricks_notebook" "transform_to_silver" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../job_notebooks/TransformToSilver.py"))
  path           = "/Workspace/TransformToSilver"
  language       = "PYTHON"
}

resource "databricks_notebook" "aggregate_to_gold" {
  provider = databricks.workspace
  content_base64 = base64encode(file("../job_notebooks/AggregateToGold.py"))
  path           = "/Workspace/AggregateToGold"
  language       = "PYTHON"
}

resource "databricks_job" "workflow" {
  provider = databricks.workspace
  name = "ETL Workflow"
  job_cluster {
    job_cluster_key = "etl_job_cluster"
    new_cluster {
      num_workers   = 2
      spark_version = "14.3.x-scala2.12"
      node_type_id  = "Standard_DS3_v2"
    }
  }

  task {
    task_key       = "GenerateRawData"
    job_cluster_key = "etl_job_cluster"
    notebook_task {
      notebook_path = databricks_notebook.generate_raw_data.path
    }
  }

  task {
    task_key       = "TransformToSilver"
    job_cluster_key = "etl_job_cluster"
    depends_on {
      task_key = "GenerateRawData"
    }
    notebook_task {
      notebook_path = databricks_notebook.transform_to_silver.path
    }
  }

  task {
    task_key       = "AggregateToGold"
    job_cluster_key = "etl_job_cluster"
    depends_on {
      task_key = "TransformToSilver"
    }
    notebook_task {
      notebook_path = databricks_notebook.aggregate_to_gold.path
    }
  }

  schedule {
    quartz_cron_expression = "0 0 * * * ?"
    timezone_id            = "UTC"
    pause_status           = "PAUSED"
  }
}
