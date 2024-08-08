output "resource_group_east_name" {
  description = "The name of the East US resource group"
  value       = azurerm_resource_group.dr_test_east_us.name
}

output "east_storage_account_name" {
  description = "The name of the East US storage account"
  value       = azurerm_storage_account.dr_test_east_storage.name
}

output "east_databricks_workspace_name" {
  description = "The name of the East US Databricks workspace"
  value       = azurerm_databricks_workspace.dr_test_east_databricks.name
}


output "east_external_container_name" {
  description = "The name of the external container in the East US storage account"
  value       = azurerm_storage_container.dr_test_east_external.name
}

output "east_dr_sync_container_name" {
  description = "The name of the dr_sync container in the East US storage account"
  value       = azurerm_storage_container.dr_test_east_dr_sync.name
}

output "east_databricks_workspace_url" {
  value = azurerm_databricks_workspace.dr_test_east_databricks.workspace_url
}
