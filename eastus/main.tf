provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "dr_test_eastus2" {
  name     = "dr_test_eastus2"
  location = var.location

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}
