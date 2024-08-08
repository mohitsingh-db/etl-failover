resource "databricks_service_principal" "dr_test_sp" {
  display_name = "dr_test_sp"
}

resource "databricks_grants" "catalog_grants" {
  catalog = databricks_catalog.dr_test_catalog.name

  depends_on = [
    databricks_service_principal.dr_test_sp
  ]

  grant {
    principal  = "mohit.singh@databricks.com"
    privileges = ["ALL_PRIVILEGES"]
  }
}

