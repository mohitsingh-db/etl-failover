# Databricks notebook source
import unittest
import os
from config_loader import load_config

class TestConfigLoader(unittest.TestCase):
    def test_load_config(self):
        config = load_config('./config/config.json')
        self.assertIn('sync_location', config)
        self.assertEqual(config['sync_location'], 'abfss://container@storageaccount.dfs.core.windows.net/')

suite = unittest.TestLoader().loadTestsFromTestCase(TestConfigLoader)
unittest.TextTestRunner().run(suite)

# COMMAND ----------

class TestValidator(unittest.TestCase):
    def test_validate_config(self):
        # Define a sample valid config for testing
        valid_config = {
            "sync_location": "abfss://conatiner@storageaccount.dfs.core.windows.net/",
            "sync_failure_notification": "mohit.singh@databricks.com",
            "sync_success_notification": "mohit.singh@databricks.com",
            "sync_default_schedule": "* * * * *",
            "sync_max_retries": "",
            "sync_groups": [
                {
                    "group_name": "core_product_1",
                    "group_owner_email": "mohit.singh@databricks.com",
                    "workflow": "",
                    "tables": []
                },
                {
                    "group_name": "core_product_2",
                    "group_owner_email": "mohit.singh@databricks.com",
                    "workflow": "",
                    "tables": []
                }
            ]
        }

        invalid_config_duplicate_groups = {
            "sync_location": "abfss://conatiner@storageaccount.dfs.core.windows.net/",
            "sync_failure_notification": "mohit.singh@databricks.com",
            "sync_success_notification": "mohit.singh@databricks.com",
            "sync_default_schedule": "* * * * *",
            "sync_max_retries": "",
            "sync_groups": [
                {
                    "group_name": "core_product",
                    "group_owner_email": "mohit.singh@databricks.com",
                    "workflow": "",
                    "tables": []
                },
                {
                    "group_name": "core_product",
                    "group_owner_email": "mohit.singh@databricks.com",
                    "workflow": "",
                    "tables": []
                }
            ]
        }

        invalid_config_missing_sync_location = {
            "sync_location": "",
            "sync_failure_notification": "mohit.singh@databricks.com",
            "sync_success_notification": "mohit.singh@databricks.com",
            "sync_default_schedule": "* * * * *",
            "sync_max_retries": "",
            "sync_groups": [
                {
                    "group_name": "core_product",
                    "group_owner_email": "mohit.singh@databricks.com",
                    "workflow": "",
                    "tables": []
                }
            ]
        }

        # Test valid config
        self.assertTrue(validate_config(valid_config))

        # Test invalid config with duplicate group names
        self.assertFalse(validate_config(invalid_config_duplicate_groups))

        # Test invalid config with missing sync location
        self.assertFalse(validate_config(invalid_config_missing_sync_location))

# Run the test case
suite = unittest.TestLoader().loadTestsFromTestCase(TestValidator)
unittest.TextTestRunner().run(suite)


# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""