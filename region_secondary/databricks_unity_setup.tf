terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.48.3"  # Use the latest version
    }
  }
}

provider "databricks" {
  alias                     = "account"
  account_id                = "ccb842e7-2376-4152-b0b0-29fa952379b8"
  host     = "https://accounts.azuredatabricks.net/"
}

provider "databricks" {
  alias                     = "workspace"
  host  = azurerm_databricks_workspace.dr_test_westus2_databricks.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.dr_test_westus2_databricks.id
}

resource "databricks_metastore" "dr_westus2_metastore" {
  provider = databricks.account
  name     = "dr_westus2_metastore"
  region   = var.region
}

resource "databricks_metastore_assignment" "dr_westus2_metastore_assignment" {
  provider            = databricks.workspace
  workspace_id        = azurerm_databricks_workspace.dr_test_westus2_databricks.workspace_id
  metastore_id        = databricks_metastore.dr_westus2_metastore.metastore_id
  default_catalog_name = "main"

  depends_on = [azurerm_databricks_workspace.dr_test_westus2_databricks, databricks_metastore.dr_westus2_metastore]
}


resource "databricks_storage_credential" "dr_westus2_storage_credential" {
  provider            = databricks.workspace
  name = "dr_westus2_storage_credential"
  
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.dr_test_westus2_connector.id
  }

  comment = "Managed identity for storage access"
  depends_on = [databricks_metastore_assignment.dr_westus2_metastore_assignment]
}

resource "databricks_external_location" "dr_westus2_external_location" {
  provider            = databricks.workspace
  name            = "dr_westus2_external_location"
  url             = "abfss://${azurerm_storage_container.dr_test_westus2_external.name}@${azurerm_storage_account.dr_test_westus2_storage.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dr_westus2_storage_credential.name
  comment         = "External location for data storage in westus2"
}

resource "databricks_external_location" "dr_westus2_sync_location" {
  provider            = databricks.workspace
  name            = "dr_westus2_sync_location"
  url             = "abfss://${azurerm_storage_container.dr_test_westus2_dr_sync.name}@${azurerm_storage_account.dr_test_westus2_storage.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dr_westus2_storage_credential.name
  comment         = "Sync location for data storage in westus2"
}

resource "databricks_catalog" "dr_test_catalog" {
  provider            = databricks.workspace
  name               = "dr_test_catalog"
  comment            = "Test catalog for data organization"
  storage_root = databricks_external_location.dr_westus2_external_location.url

}


resource "databricks_schema" "raw" {
  provider = databricks.workspace
  name       = "raw"
  catalog_name = databricks_catalog.dr_test_catalog.name
  comment    = "Schema for raw data"
}

resource "databricks_schema" "stage" {
  provider = databricks.workspace
  name       = "stage"
  catalog_name = databricks_catalog.dr_test_catalog.name
  comment    = "Schema for silver data"
}

resource "databricks_schema" "main" {
  provider = databricks.workspace
  name       = "main"
  catalog_name = databricks_catalog.dr_test_catalog.name
  comment    = "Schema for gold data"
}


data "terraform_remote_state" "eastus2" {
  backend = "local"
  config = {
    path = "../region_primary/terraform.tfstate"
  }

  depends_on = [databricks_schema.main]
}

resource "databricks_storage_credential" "dr_cross_eastus2_storage_credential" {
  provider            = databricks.workspace
  name = "dr_cross_eastus2_storage_credential"
  
  azure_managed_identity {
    access_connector_id = data.terraform_remote_state.eastus2.outputs.eastus2_access_connector_id
  }
}

resource "databricks_external_location" "dr_cross_eastus2_sync_location" {
  provider            = databricks.workspace
  name            = "dr_cross_eastus2_sync_location"
  url             = "abfss://${data.terraform_remote_state.eastus2.outputs.eastus2_dr_sync_container_name}@${data.terraform_remote_state.eastus2.outputs.eastus2_storage_account_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dr_cross_eastus2_storage_credential.name
  comment         = "Sync location for data storage in eastus2"
}