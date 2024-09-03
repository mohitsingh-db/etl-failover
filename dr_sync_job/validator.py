# Databricks notebook source
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

    # Ensure 'secret_scope' is present
    secret_scope = config.get("secret_scope")
    if not secret_scope:
        log_message("debug","Error: 'secret_scope' is missing. Stopping job.")
        return {}
    
    validated_config["secret_scope"] = secret_scope
    
    for workspace in config['workspaces']:
        log_message("debug",f"Validating workspace: {workspace.get('workspace_name', 'Unnamed')}")

        # Check critical fields
        if not workspace.get('workspace_name'):
            log_message("debug","Error: Workspace name is missing. Stopping job.")
            return {}

        if not workspace.get('workspace_instance_url'):
            log_message("debug",f"Error: Workspace instance URL is missing for workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        if not workspace.get('workspace_pat_key'):
            log_message("debug",f"Error: Workspace PAT key is missing for workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        if not workspace.get('sync_location_primary'):
            log_message("debug",f"Error: primary region sync location is missing for workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        if not workspace.get('sync_location_secondary'):
            log_message("debug",f"Error: secondary region sync location is missing for workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        # Handle optional fields with defaults
        if not workspace.get('sync_interval_mins'):
            log_message("debug",f"Warning: Sync default schedule is missing for workspace '{workspace['workspace_name']}'. Using default schedule: every 12 hours.")
            workspace['sync_interval_mins'] = 720

        if not workspace.get('sync_failover_interval_mins'):
            log_message("debug",f"Warning: Sync default schedule is missing for workspace '{workspace['workspace_name']}'. Using default schedule: every 12 hours.")
            workspace['sync_interval_mins'] = 2160

        if not workspace.get('sync_max_retries'):
            log_message("debug",f"Warning: Sync max retries is missing for workspace '{workspace['workspace_name']}'. Using default max retries: 3.")
            workspace['sync_max_retries'] = "3"

        # Validate group names for uniqueness
        group_names = [group.get('group_name') for group in workspace['sync_groups'] if group.get('group_name')]
        if len(group_names) != len(set(group_names)):
            log_message("debug",f"Error: Group names must be unique within workspace '{workspace['workspace_name']}'. Stopping job.")
            return {}

        # Validate each sync group
        validated_groups = []
        for group in workspace['sync_groups']:
            group_name = group.get('group_name', 'Unnamed')

            if not group_name:
                log_message("debug",f"Warning: Sync group name is not defined in workspace '{workspace['workspace_name']}'. Continuing with this group.")

            # Exception for 'scheduled_sync' group: Skip workflow validation
            if group_name != 'scheduled_sync' and not group.get('workflow'):
                log_message("debug",f"Warning: Sync group '{group_name}' in workspace '{workspace['workspace_name']}' does not have a workflow defined.")

            if not group.get('tables'):
                log_message("debug",f"Warning: Sync group '{group_name}' in workspace '{workspace['workspace_name']}' does not have any tables defined.")

            if group_name and (group.get('workflow') or group.get('tables') or group_name == 'scheduled_sync'):
                validated_groups.append(group)

        workspace['sync_groups'] = validated_groups
        validated_config['workspaces'].append(workspace)

    # Print a summary of the validation
    log_message("debug","\nValidation Summary:")
    log_message("debug",f"{'Workspace Name':<25} {'Groups Validated':<20} {'Default Schedule':<20} {'Max Retries':<10}")
    for workspace in validated_config['workspaces']:
        num_groups = len(workspace['sync_groups'])
        log_message("debug",f"{workspace['workspace_name']:<25} {num_groups:<20} {workspace['sync_interval_mins']:<20} {workspace['sync_max_retries']:<10}")

    return validated_config

def normalize(value):
    return value if value is not None else ""

def validate_workflow_and_tables(config: dict, sync_location_to: str = "primary") -> bool:
    """
    This function performs detailed validation of workflows and tables within each workspace.

    - Validates access to external storage locations.
    - Ensures the existence and accessibility of metadata tables at specific subpaths in the storage locations.
    - Validates workflows and tables that haven't been validated yet, according to a pre-existing metadata table.
    - Collects validation results and merges them into the metadata table in one shot.
    - Logs issues and continues validation for other groups even if some fail.

    Args:
    - config (dict): The configuration dictionary loaded from the JSON file.
    - sync_location_to (str): Either 'primary' or 'secondary' to specify which sync location to validate.

    Returns:
    - bool: True if all workflows and tables are valid, False otherwise.
    """
    all_valid = True
    new_metadata_entries = []  # Collect new or updated metadata entries

    # Determine whether to use primary or secondary location
    if sync_location_to not in ["primary", "secondary"]:
        raise ValueError("sync_location_to must be either 'primary' or 'secondary'")

    for workspace in config['workspaces']:
        log_message("debug",f"Validating workspace: {workspace.get('workspace_name', 'Unnamed')}")

        # Determine sync location based on parameter
        sync_location = workspace.get(f"sync_location_{sync_location_to}")
        meta_path = f"{sync_location}/meta"

        # Check access to the sync location
        if not check_fs_path(meta_path):
            log_message("debug",f"Error: No access to 'meta' path at {sync_location_to} sync location '{meta_path}' for workspace '{workspace['workspace_name']}'.")
            all_valid = False
            continue

        # Load metadata from the specified location
        metadata_table_path = f"{meta_path}/sync_metadata_table"
        metadata_table = load_metadata_table(metadata_table_path)

        for group in workspace['sync_groups']:
            group_name = group.get('group_name', 'Unnamed')

            # Check if this group is already validated in the metadata table
            validation_entry = metadata_table.get((workspace['workspace_name'], group_name), None)
            log_message("debug",f"debug: validating sync group '{group_name}'. found entry {validation_entry}")
            
            # Handle potential deletions and changes in configuration
            if validation_entry:
                # If the group has been deleted from the configuration, skip adding it to the new entries
                validation_entry['workspace_name'] = workspace['workspace_name']
                validation_entry['group_name'] = group_name
                if not any(g['group_name'] == group_name for g in workspace['sync_groups']):
                    log_message("debug",f"Info: Sync group '{group_name}' no longer exists in configuration. Removing from metadata table.")
                    continue

                # Check for changes in workflow or tables
                if (normalize(validation_entry['workflow_name']) != normalize(group.get('workflow'))) or (validation_entry['tables'] != group.get('tables')):
                    log_message("debug",f"Info: Detected changes in sync group '{group_name}' configuration. Revalidating.")
                    validation_entry['validated'] = False  # Mark as not validated for revalidation

            if validation_entry and validation_entry['validated']:
                log_message("debug",f"Info: Sync group '{group_name}' in workspace '{workspace['workspace_name']}' is already validated. Skipping validation.")
                new_metadata_entries.append(validation_entry)
                continue

            # Validate workflow existence
            if group.get('workflow'):
                try:
                    # Fetch the job info using the provided method, retrieve PAT token and scope from config
                    job_info = get_databricks_jobs_info(
                        workspace['workspace_instance_url'], 
                        config.get('secret_scope'),  # Pass the secret scope from config
                        workspace['workspace_pat_key'], 
                        [group['workflow']]
                    )

                    if not job_info:
                        log_message("debug",f"Error: Workflow '{group['workflow']}' does not exist for group '{group_name}' in workspace '{workspace['workspace_name']}'.")
                        all_valid = False
                        continue
                    else:
                        workflow_id = job_info.get(group['workflow'], None)
                        validation_entry = {
                            'workspace_name': workspace['workspace_name'],
                            'group_name': group_name,
                            'workflow_name': group.get('workflow'),
                            'workflow_id': workflow_id,
                            'tables': group.get('tables', []),
                            'validated': False  # Mark as not validated until re-validation is done
                        }
                except Exception as e:
                    log_message("debug",f"Error: Could not validate workflow '{group['workflow']}' for group '{group_name}' in workspace '{workspace['workspace_name']}': {e}")
                    all_valid = False
                    continue
            else:
                validation_entry = {
                            'workspace_name': workspace['workspace_name'],
                            'group_name': group_name,
                            'workflow_name': "",
                            'workflow_id': "",
                            'tables': group.get('tables', []),
                            'validated': False  # Mark as not validated until re-validation is done
                        }
                        
            # Validate tables existence and read permissions
            for table in group['tables']:
                try:
                    if not check_table(table):
                        log_message("debug",f"Error: Cannot access table '{table}' for group '{group_name}' in workspace '{workspace['workspace_name']}'.")
                        all_valid = False
                except Exception as e:
                    log_message("debug",f"Error: Could not validate table '{table}' for group '{group_name}' in workspace '{workspace['workspace_name']}': {e}")
                    all_valid = False

            # If validation was successful, update the metadata entry as validated
            if all_valid:
                validation_entry['validated'] = True
                new_metadata_entries.append(validation_entry)

    # Once all validations are done, merge new entries into the metadata table, only if new entries were found
    if new_metadata_entries:
        merge_metadata_entries(metadata_table_path, new_metadata_entries)
    else:
        log_message("debug","No new metadata entries found. Skipping metadata merge.")

    return all_valid
