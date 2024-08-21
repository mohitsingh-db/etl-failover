resource "azurerm_databricks_access_connector" "dr_test_centralus_connector" {
  name                = "dr_test_centralus_connector"
  resource_group_name = azurerm_resource_group.dr_test_centralus.name
  location            = azurerm_resource_group.dr_test_centralus.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}



resource "azurerm_role_assignment" "centralus_blob_contributor" {
  principal_id         = azurerm_databricks_access_connector.dr_test_centralus_connector.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = azurerm_storage_account.dr_test_centralus_storage.id

  depends_on = [
    azurerm_databricks_access_connector.dr_test_centralus_connector
  ]
}

resource "azurerm_role_assignment" "centralus_queue_contributor" {
  principal_id         = azurerm_databricks_access_connector.dr_test_centralus_connector.identity[0].principal_id
  role_definition_name = "Storage Queue Data Contributor"
  scope                = azurerm_storage_account.dr_test_centralus_storage.id

  depends_on = [
    azurerm_databricks_access_connector.dr_test_centralus_connector
  ]
}
