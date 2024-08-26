output "resource_group_westus2_name" {
  description = "The name of the westus2 resource group"
  value       = azurerm_resource_group.dr_test_westus2.name
}

output "westus2_storage_account_name" {
  description = "The name of the westus2 storage account"
  value       = azurerm_storage_account.dr_test_westus2_storage.name
}

output "westus2_databricks_workspace_name" {
  description = "The name of the westus2 Databricks workspace"
  value       = azurerm_databricks_workspace.dr_test_westus2_databricks.name
}


output "westus2_external_container_name" {
  description = "The name of the external container in the westus2 storage account"
  value       = azurerm_storage_container.dr_test_westus2_external.name
}

output "westus2_dr_sync_container_name" {
  description = "The name of the dr_sync container in the westus2 storage account"
  value       = azurerm_storage_container.dr_test_westus2_dr_sync.name
}

output "westus2_databricks_workspace_url" {
  value = azurerm_databricks_workspace.dr_test_westus2_databricks.workspace_url
}

output "westus2_access_connector_id" {
  value = azurerm_databricks_access_connector.dr_test_westus2_connector.id
}
