resource "azurerm_databricks_access_connector" "dr_test_eastus2_connector" {
  name                = "dr_test_eastus2_connector"
  resource_group_name = azurerm_resource_group.dr_test_eastus2.name
  location            = azurerm_resource_group.dr_test_eastus2.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}



resource "azurerm_role_assignment" "eastus2_blob_contributor" {
  principal_id         = azurerm_databricks_access_connector.dr_test_eastus2_connector.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.dr_test_eastus2_storage.id

  depends_on = [
    azurerm_databricks_access_connector.dr_test_eastus2_connector
  ]
}

resource "azurerm_role_assignment" "eastus2_queue_contributor" {
  principal_id         = azurerm_databricks_access_connector.dr_test_eastus2_connector.identity[0].principal_id
  role_definition_name = "Storage Queue Data Contributor"
  scope                = azurerm_storage_account.dr_test_eastus2_storage.id

  depends_on = [
    azurerm_databricks_access_connector.dr_test_eastus2_connector
  ]
}
