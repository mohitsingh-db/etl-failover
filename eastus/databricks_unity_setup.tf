terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.48.3"  # Use the latest version
    }
  }
}


provider "databricks" {
  host  = azurerm_databricks_workspace.dr_test_east_databricks.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.dr_test_east_databricks.id
}


resource "databricks_storage_credential" "dr_east_storage_credential" {
  name = "dr_east_storage_credential"
  
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.dr_test_east_connector.id
  }

  comment = "Managed identity for storage access"
}

resource "databricks_external_location" "dr_east_external_location" {
  name            = "dr_east_external_location"
  url             = "abfss://${azurerm_storage_container.dr_test_east_external.name}@${azurerm_storage_account.dr_test_east_storage.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dr_east_storage_credential.name
  comment         = "External location for data storage in East US"
}

resource "databricks_catalog" "dr_test_catalog" {
  name               = "dr_test_catalog"
  comment            = "Test catalog for data organization"
  storage_root = databricks_external_location.dr_east_external_location.url

}


resource "databricks_schema" "dr_test_raw" {
  name       = "dr_test_raw"
  catalog_name = databricks_catalog.dr_test_catalog.name
  comment    = "Schema for raw data"
}

resource "databricks_schema" "dr_test_silver" {
  name       = "dr_test_silver"
  catalog_name = databricks_catalog.dr_test_catalog.name
  comment    = "Schema for silver data"
}

resource "databricks_schema" "dr_test_gold" {
  name       = "dr_test_gold"
  catalog_name = databricks_catalog.dr_test_catalog.name
  comment    = "Schema for gold data"
}
