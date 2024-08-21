provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "dr_test_centralus" {
  name     = "dr_test_centralus"
  location = var.location

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}
