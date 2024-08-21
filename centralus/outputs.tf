output "resource_group_centralus_name" {
  description = "The name of the centralus resource group"
  value       = azurerm_resource_group.dr_test_centralus.name
}

output "centralus_storage_account_name" {
  description = "The name of the centralus storage account"
  value       = azurerm_storage_account.dr_test_centralus_storage.name
}

output "centralus_databricks_workspace_name" {
  description = "The name of the centralus Databricks workspace"
  value       = azurerm_databricks_workspace.dr_test_centralus_databricks.name
}


output "centralus_external_container_name" {
  description = "The name of the external container in the centralus storage account"
  value       = azurerm_storage_container.dr_test_centralus_external.name
}

output "centralus_dr_sync_container_name" {
  description = "The name of the dr_sync container in the centralus storage account"
  value       = azurerm_storage_container.dr_test_centralus_dr_sync.name
}

output "centralus_databricks_workspace_url" {
  value = azurerm_databricks_workspace.dr_test_centralus_databricks.workspace_url
}

output "centralus_access_connector_id" {
  value = azurerm_databricks_access_connector.dr_test_centralus_connector.id
}
