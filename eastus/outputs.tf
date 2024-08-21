output "resource_group_eastus2_name" {
  description = "The name of the eastus2 resource group"
  value       = azurerm_resource_group.dr_test_eastus2.name
}

output "eastus2_storage_account_name" {
  description = "The name of the eastus2 storage account"
  value       = azurerm_storage_account.dr_test_eastus2_storage.name
}

output "eastus2_databricks_workspace_name" {
  description = "The name of the eastus2 Databricks workspace"
  value       = azurerm_databricks_workspace.dr_test_eastus2_databricks.name
}


output "eastus2_external_container_name" {
  description = "The name of the external container in the eastus2 storage account"
  value       = azurerm_storage_container.dr_test_eastus2_external.name
}

output "eastus2_dr_sync_container_name" {
  description = "The name of the dr_sync container in the eastus2 storage account"
  value       = azurerm_storage_container.dr_test_eastus2_dr_sync.name
}

output "eastus2_databricks_workspace_url" {
  value = azurerm_databricks_workspace.dr_test_eastus2_databricks.workspace_url
}

output "eastus2_access_connector_id" {
  value = azurerm_databricks_access_connector.dr_test_eastus2_connector.id
}
