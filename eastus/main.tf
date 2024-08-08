provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "dr_test_east_us" {
  name     = "dr_test_east_us"
  location = "East US"

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}
