resource "azurerm_databricks_workspace" "dr_test_centralus_databricks" {
  name                = "dr_test_centralus_databricks"
  resource_group_name = azurerm_resource_group.dr_test_centralus.name
  location            = azurerm_resource_group.dr_test_centralus.location
  sku                 = "premium"

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}
