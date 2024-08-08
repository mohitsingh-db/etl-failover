resource "azurerm_databricks_access_connector" "dr_test_central_connector" {
  name                = "dr_test_central_connector"
  resource_group_name = azurerm_resource_group.dr_test_central_us.name
  location            = azurerm_resource_group.dr_test_central_us.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}



resource "azurerm_role_assignment" "central_blob_contributor" {
  principal_id         = azurerm_databricks_access_connector.dr_test_central_connector.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.dr_test_central_storage.id

  depends_on = [
    azurerm_databricks_access_connector.dr_test_central_connector
  ]
}

resource "azurerm_role_assignment" "central_queue_contributor" {
  principal_id         = azurerm_databricks_access_connector.dr_test_central_connector.identity[0].principal_id
  role_definition_name = "Storage Queue Data Contributor"
  scope                = azurerm_storage_account.dr_test_central_storage.id

  depends_on = [
    azurerm_databricks_access_connector.dr_test_central_connector
  ]
}
