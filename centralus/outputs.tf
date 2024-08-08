
output "resource_group_central_name" {
  description = "The name of the Central US resource group"
  value       = azurerm_resource_group.dr_test_central_us.name
}

output "central_storage_account_name" {
  description = "The name of the Central US storage account"
  value       = azurerm_storage_account.dr_test_central_storage.name
}

output "central_databricks_workspace_name" {
  description = "The name of the Central US Databricks workspace"
  value       = azurerm_databricks_workspace.dr_test_central_databricks.name
}


output "central_external_container_name" {
  description = "The name of the external container in the Central US storage account"
  value       = azurerm_storage_container.dr_test_central_external.name
}

output "central_dr_sync_container_name" {
  description = "The name of the dr_sync container in the Central US storage account"
  value       = azurerm_storage_container.dr_test_central_dr_sync.name
}

output "central_databricks_workspace_url" {
  value = azurerm_databricks_workspace.dr_test_central_databricks.workspace_url
}
