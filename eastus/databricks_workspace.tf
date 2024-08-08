resource "azurerm_databricks_workspace" "dr_test_east_databricks" {
  name                = "dr_test_east_databricks"
  resource_group_name = azurerm_resource_group.dr_test_east_us.name
  location            = azurerm_resource_group.dr_test_east_us.location
  sku                 = "premium"

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}

