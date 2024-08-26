resource "azurerm_storage_account" "dr_test_westus2_storage" {
  name                     = "westus2drstorageacct" # Ensure this name is globally unique
  resource_group_name      = azurerm_resource_group.dr_test_westus2.name
  location                 = azurerm_resource_group.dr_test_westus2.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}

resource "azurerm_storage_container" "dr_test_westus2_external" {
  name                  = "external"
  storage_account_name  = azurerm_storage_account.dr_test_westus2_storage.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "dr_test_westus2_dr_sync" {
  name                  = "dr-sync"
  storage_account_name  = azurerm_storage_account.dr_test_westus2_storage.name
  container_access_type = "private"
}
