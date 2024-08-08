provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "dr_test_central_us" {
  name     = "dr_test_central_us"
  location = "Central US"

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}
