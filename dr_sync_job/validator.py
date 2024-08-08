import json
import requests  # Assuming you can use requests to interact with Databricks API
from databricks_api import DatabricksAPI  # You might need a suitable library for interacting with Databricks

# Initialize Databricks API client (replace with actual configuration)
databricks_instance = 'https://<databricks-instance>'
api_token = '<api-token>'
db = DatabricksAPI(host=databricks_instance, token=api_token)

def validate_config(config: dict) -> bool:
    # Basic validation
    group_names = [group['group_name'] for group in config['sync_groups']]
    if len(group_names) != len(set(group_names)):
        print("Group names must be unique")
        return False

    if not config['sync_location']:
        print("Sync location is missing")
        return False

    if not config['sync_default_schedule']:
        print("Sync default schedule is missing")
        return False

    for group in config['sync_groups']:
        if not group['tables']:
            print(f"Group {group['group_name']} must have at least one table")
            return False

    return True

def validate_workflow_and_tables(config: dict) -> bool:
    all_valid = True

    for group in config['sync_groups']:
        # Validate workflow existence
        if group['workflow']:
            try:
                workflow_info = db.jobs.get_job(group['workflow'])
                if not workflow_info:
                    print(f"Workflow {group['workflow']} does not exist for group {group['group_name']}")
                    all_valid = False
            except Exception as e:
                print(f"Error checking workflow {group['workflow']}: {e}")
                all_valid = False

        # Validate tables existence and read permissions
        for table in group['tables']:
            try:
                table_info = db.tables.get_table(table)
                if not table_info:
                    print(f"Table {table} does not exist for group {group['group_name']}")
                    all_valid = False
                else:
                    # Check read permission (dummy check, replace with actual logic)
                    if not db.tables.has_read_permission(table):
                        print(f"No read permission for table {table} in group {group['group_name']}")
                        all_valid = False
            except Exception as e:
                print(f"Error checking table {table}: {e}")
                all_valid = False

    # Validate sync location accessibility (dummy check, replace with actual logic)
    try:
        response = requests.get(config['sync_location'])
        if response.status_code != 200:
            print(f"Sync location {config['sync_location']} is not accessible")
            all_valid = False
    except Exception as e:
        print(f"Error accessing sync location {config['sync_location']}: {e}")
        all_valid = False

    return all_valid
