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