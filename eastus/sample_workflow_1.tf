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

# Create an all-purpose single-node cluster with specified configuration for Azure
resource "databricks_cluster" "etl_cluster" {
  cluster_name        = "Etl Cluster"
  spark_version       = "14.3.x-scala2.12"
  num_workers         = 0
  node_type_id        = "Standard_D4ds_v5"
  autotermination_minutes = 20
  enable_elastic_disk = true
  runtime_engine      = "PHOTON"
  single_user_name    = "mohit.singh@databricks.com"
  data_security_mode  = "SINGLE_USER"

  spark_conf = {
    "spark.master"                          = "local[*, 4]"
    "spark.databricks.cluster.profile"      = "singleNode"
  }

  azure_attributes {
    first_on_demand        = 1
    availability           = "ON_DEMAND_AZURE"
    spot_bid_max_price     = -1
  }

  custom_tags = {
    "ResourceClass" = "SingleNode"
  }

  spark_env_vars = {
    "PYSPARK_PYTHON" = "/databricks/python3/bin/python3"
  }

  provider = databricks.workspace
}

# Create a Databricks Job (workflow) with multiple tasks using the all-purpose cluster
resource "databricks_job" "sample_workflow_1" {
  name = "sample_workflow_1"
  max_concurrent_runs = 1
  provider = databricks.workspace

  parameter {
    name   = "storageaccname"
    default = azurerm_storage_account.dr_test_eastus2_storage.name
  }

  task {
    task_key = "file_checker"
    notebook_task {
      notebook_path = "/Workspace/sample_workflow_1/file_checker"
    }
    existing_cluster_id = databricks_cluster.etl_cluster.id
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
    existing_cluster_id = databricks_cluster.etl_cluster.id
  }

  task {
    task_key = "raw_to_stage"
    depends_on {
      task_key = "adls_to_raw"
    }
    notebook_task {
      notebook_path = "/Workspace/sample_workflow_1/silver"
    }
    existing_cluster_id = databricks_cluster.etl_cluster.id
  }

  task {
    task_key = "archive"
    depends_on {
      task_key = "raw_to_stage"
    }
    notebook_task {
      notebook_path = "/Workspace/sample_workflow_1/archive"
    }
    existing_cluster_id = databricks_cluster.etl_cluster.id
  }

  task {
    task_key = "stage_to_main"
    depends_on {
      task_key = "archive"
    }
    notebook_task {
      notebook_path = "/Workspace/sample_workflow_1/gold"
    }
    existing_cluster_id = databricks_cluster.etl_cluster.id
  }
}
