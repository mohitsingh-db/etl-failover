provider "azurerm" {
  features {}
}

resource "azurerm_resource_group" "dr_test_westus2" {
  name     = "dr_test_westus2"
  location = var.location

  tags = {
    Owner = "mohit.singh@databricks.com"
  }
}
