from utils import check_fs_path, load_metadata_table, get_databricks_job_info, check_table

def validate_config(config: dict) -> dict:
    """
    This function performs basic validation of the configuration JSON.

    - Ensures that critical fields are not empty and stops the job if they are.
    - Ensures that group names within each workspace are unique.
    - Provides default values for optional fields if they are missing.
    - Warns and continues if optional fields or conditions are not met.
    - Removes invalid sync groups from the configuration.
    - Prints a summary in a tabular format.

    Args:
    - config (dict): The configuration dictionary loaded from the JSON file.

    Returns:
    - dict: The validated and potentially modified configuration dictionary.
    """
    validated_config = {"workspaces": []}
    for workspace in config['workspaces']:
        print(f"Validating workspace: {workspace.get('workspace_name', 'Unnamed')}")

        # Check critical fields
        if not workspace.get('workspace_name'):
            print("Error: Workspace name is missing. Stopping job.")
            return {}

        if not workspace.get('workspace_instance_url'):
            print(f"Error: Workspace instance URL is missing for workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        if not workspace.get('workspace_pat_key'):
            print(f"Error: Workspace PAT key is missing for workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        if not workspace.get('sync_location_primary'):
            print(f"Error: primary region sync location is missing for workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        if not workspace.get('sync_location_secondary'):
            print(f"Error: secondary region sync location is missing for workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        # Handle optional fields with defaults
        if not workspace.get('sync_default_schedule'):
            print(f"Warning: Sync default schedule is missing for workspace '{workspace['workspace_name']}'. Using default schedule: every 12 hours.")
            workspace['sync_default_schedule'] = "0 */12 * * *"

        if not workspace.get('sync_max_retries'):
            print(f"Warning: Sync max retries is missing for workspace '{workspace['workspace_name']}'. Using default max retries: 3.")
            workspace['sync_max_retries'] = "3"

        # Validate group names for uniqueness
        group_names = [group.get('group_name') for group in workspace['sync_groups'] if group.get('group_name')]
        if len(group_names) != len(set(group_names)):
            print(f"Error: Group names must be unique within workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        # Validate each sync group
        validated_groups = []
        for group in workspace['sync_groups']:
            group_name = group.get('group_name', 'Unnamed')

            if not group_name:
                print(f"Warning: Sync group name is not defined in workspace '{workspace['workspace_name']}'. Continuing with this group.")

            # Exception for 'scheduled_sync' group: Skip workflow validation
            if group_name != 'scheduled_sync' and not group.get('workflow'):
                print(f"Warning: Sync group '{group_name}' in workspace '{workspace['workspace_name']}' does not have a workflow defined.")

            if not group.get('tables'):
                print(f"Warning: Sync group '{group_name}' in workspace '{workspace['workspace_name']}' does not have any tables defined.")

            if group_name and (group.get('workflow') or group.get('tables') or group_name == 'scheduled_sync'):
                validated_groups.append(group)

        workspace['sync_groups'] = validated_groups
        validated_config['workspaces'].append(workspace)

    # Print a summary of the validation
    print("\nValidation Summary:")
    print(f"{'Workspace Name':<25} {'Groups Validated':<20} {'Default Schedule':<20} {'Max Retries':<10}")
    for workspace in validated_config['workspaces']:
        num_groups = len(workspace['sync_groups'])
        print(f"{workspace['workspace_name']:<25} {num_groups:<20} {workspace['sync_default_schedule']:<20} {workspace['sync_max_retries']:<10}")

    return validated_config

def validate_workflow_and_tables(config: dict) -> bool:
    """
    This function performs detailed validation of workflows and tables within each workspace.

    - Validates access to external storage locations.
    - Ensures the existence and accessibility of metadata tables at specific subpaths in the storage locations.
    - Validates workflows and tables that haven't been validated yet, according to a pre-existing metadata table.
    - Updates the metadata table upon successful validation.
    - Logs issues and continues validation for other groups even if some fail.

    Args:
    - config (dict): The configuration dictionary loaded from the JSON file.

    Returns:
    - bool: True if all workflows and tables are valid, False otherwise.
    """
    all_valid = True

    for workspace in config['workspaces']:
        print(f"Validating workspace: {workspace.get('workspace_name', 'Unnamed')}")

        # Check if the 'meta' subpath exists and has write access in both locations
        meta_primary_path = f"{workspace.get('sync_location_primary')}/meta"
        meta_secondary_path = f"{workspace.get('sync_location_secondary')}/meta"
        
        # Check access to primary and secondary sync locations
        if not check_fs_path(meta_primary_path):
            print(f"Error: No access to 'meta' path at primary sync location '{meta_primary_path}' for workspace '{workspace['workspace_name']}'.")
            return False

        if not check_fs_path(meta_secondary_path):
            print(f"Error: No access to 'meta' path at secondary sync location '{meta_secondary_path}' for workspace '{workspace['workspace_name']}'.")
            return False

        # Validate and update metadata from the primary location
        metadata_table_path = f"{meta_primary_path}/sync_metadata_table"
        metadata_table = load_metadata_table(metadata_table_path)

        for group in workspace['sync_groups']:
            group_name = group.get('group_name', 'Unnamed')

            # Check if this group is already validated in the metadata table
            validation_entry = metadata_table.get((workspace['workspace_name'], group_name))

            # Handle potential deletions and changes in configuration
            if validation_entry:
                # If the group has been deleted from the configuration, remove it from the metadata table
                if not any(g['group_name'] == group_name for g in workspace['sync_groups']):
                    print(f"Info: Sync group '{group_name}' no longer exists in configuration. Removing from metadata table.")
                    delete_metadata_entry(metadata_table, workspace['workspace_name'], group_name)
                    continue

                # Check for changes in workflow or tables
                if (validation_entry['workflow'] != group.get('workflow')) or (validation_entry['tables'] != group.get('tables')):
                    print(f"Info: Detected changes in sync group '{group_name}' configuration. Revalidating.")
                    validation_entry['validated'] = False  # Mark as not validated for revalidation

            if validation_entry and validation_entry['validated']:
                print(f"Info: Sync group '{group_name}' in workspace '{workspace['workspace_name']}' is already validated. Skipping validation.")
                continue

            # Validate workflow existence
            if group.get('workflow'):
                try:
                    # Use a separate method to handle API calls for better modularity and error handling
                    workflow_info = get_databricks_job_info(workspace['workspace_instance_url'], workspace['workspace_pat_key'], group['workflow'])
                    if not workflow_info:
                        print(f"Error: Workflow '{group['workflow']}' does not exist for group '{group_name}' in workspace '{workspace['workspace_name']}'.")
                        all_valid = False
                        continue
                    else:
                        workflow_id = workflow_info.get('job_id', None)
                        validation_entry['workflow_id'] = workflow_id
                except Exception as e:
                    print(f"Error: Could not validate workflow '{group['workflow']}' for group '{group_name}' in workspace '{workspace['workspace_name']}'. {e}")
                    all_valid = False
                    continue

            # Validate tables existence and read permissions
            for table in group['tables']:
                try:
                    # Use the check_table method to verify if the table exists and the user has read permissions
                    if not check_table(table):
                        print(f"Error: Cannot access table '{table}' for group '{group_name}' in workspace '{workspace['workspace_name']}'.")
                        all_valid = False
                except Exception as e:
                    print(f"Error: Could not validate table '{table}' for group '{group_name}' in workspace '{workspace['workspace_name']}': {e}")
                    all_valid = False

            # If validation was successful, update the metadata entry as validated
            if all_valid:
                validation_entry['validated'] = True
                update_metadata_table(metadata_table, validation_entry)

    return all_valid
