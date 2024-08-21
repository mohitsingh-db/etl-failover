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
  host  = azurerm_databricks_workspace.dr_test_centralus_databricks.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.dr_test_centralus_databricks.id
}

resource "databricks_metastore" "dr_centralus_metastore" {
  provider = databricks.account
  name     = "dr_centralus_metastore"
  region   = var.region
}

resource "databricks_metastore_assignment" "dr_centralus_metastore_assignment" {
  provider            = databricks.workspace
  workspace_id        = azurerm_databricks_workspace.dr_test_centralus_databricks.workspace_id
  metastore_id        = databricks_metastore.dr_centralus_metastore.metastore_id
  default_catalog_name = "main"

  depends_on = [azurerm_databricks_workspace.dr_test_centralus_databricks, databricks_metastore.dr_centralus_metastore]
}


data "terraform_remote_state" "eastus" {
  backend = "local"
  config = {
    path = "../eastus/terraform.tfstate"  # Path to the East US state
  }
}

resource "databricks_storage_credential" "dr_centralus_storage_credential" {
  provider            = databricks.workspace
  name = "dr_centralus_storage_credential"
  
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.dr_test_centralus_connector.id
  }

  comment = "Managed identity for storage access"
  depends_on = [databricks_metastore_assignment.dr_centralus_metastore_assignment]
}

resource "databricks_external_location" "dr_centralus_external_location" {
  provider            = databricks.workspace
  name            = "dr_centralus_external_location"
  url             = "abfss://${azurerm_storage_container.dr_test_centralus_external.name}@${azurerm_storage_account.dr_test_centralus_storage.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dr_centralus_storage_credential.name
  comment         = "External location for data storage in centralus"
}

resource "databricks_catalog" "dr_test_catalog" {
  provider            = databricks.workspace
  name               = "dr_test_catalog"
  comment            = "Test catalog for data organization"
  storage_root = databricks_external_location.dr_centralus_external_location.url

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
