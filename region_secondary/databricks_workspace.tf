resource "azurerm_databricks_workspace" "dr_test_westus2_databricks" {
  name                = "dr_test_westus2_databricks"
  resource_group_name = azurerm_resource_group.dr_test_westus2.name
  location            = azurerm_resource_group.dr_test_westus2.location
  sku                 = "premium"

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}
