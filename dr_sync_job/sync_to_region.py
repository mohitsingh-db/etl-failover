# Databricks notebook source
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import pytz
from pyspark.sql import functions as F

# Global variable to collect info-level logs for successful workflows
successful_workflows = []

def process_workflow_to_region(workflow_data, instance_url, pat_token, target_location, metadata_table_path, sync_interval_mins, sync_failover_interval_mins):
    """
    Process a single workflow: Check for new commits, then do API call and deep clone if required.
    Also, update the metadata table with sync status only when there is a deep clone performed.
    """
    
    (workspace_name, group_name), metadata = workflow_data
    tables = metadata["tables"]
    validated = metadata.get("validated", None)  # Can be None if not available
    workflow_id = metadata.get("workflow_id", None)
    last_sync_time = metadata.get("last_successful_run_time", None)
    sync_status = metadata.get("sync_status", None)

    # Step 1: Parse all tables into a list of (catalog, schema, table_name)
    parsed_tables = []
    for table in tables:
        catalog, schema, table_name = table.split(".")
        parsed_tables.append((catalog, schema, table_name))

    # Step 2: Check for new commits in any of the tables
    new_commits_found = False
    try:
        for catalog, schema, table_name in parsed_tables:
            source_table = f"{catalog}.{schema}.{table_name}"

            # Query the history of the table to check for commits after the last sync
            history_df = spark.sql(f"DESCRIBE HISTORY {source_table}")

            # If last_sync_time is None, assume it's the first sync and proceed
            if last_sync_time is None or sync_status is None:
                log_message("debug", f"First time sync for {group_name}, proceeding with sync.")
                new_commits_found = True
                break  # No need to check further tables, but they will still be part of the deep clone
            else:
                # If last_sync_time is available, check for new commits
                last_sync_datetime = last_sync_time if isinstance(last_sync_time, datetime) else datetime.strptime(last_sync_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
                new_commits = history_df.filter(history_df['timestamp'] > last_sync_datetime).count()

                if new_commits > 0:
                    new_commits_found = True
                    log_message("debug", f"workflow: {group_name}, table: {source_table} Last successful time:  {last_sync_datetime} new commit found: {new_commits_found}")
                    break  # Stop checking further tables once we find new commits

        # If no new commits and not the first sync, skip the workflow without updating metadata
        if not new_commits_found:
            log_message("debug", f"No new commits found for group {group_name}, skipping.")
            return

        # Step 3: If workflow_id exists and validated, check the workflow run
        if workflow_id:
            if validated:
                successful_runs = job_successful_run_after_last_run(instance_url, pat_token, [workflow_id], last_sync_time)
                if workflow_id not in successful_runs or not successful_runs[workflow_id]:
                    log_message("debug", f"No successful runs found for workflow {workflow_id} after {last_sync_time} successful_runs are {successful_runs}. Skipping normal sync.")
                    
                    # Check if the current time minus last_sync_time exceeds sync_failover_interval_mins
                    current_time = datetime.utcnow().replace(tzinfo=pytz.UTC)
                    last_sync_datetime = last_sync_time.replace(tzinfo=pytz.UTC) if isinstance(last_sync_time, datetime) else datetime.strptime(last_sync_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
                    if (current_time - last_sync_datetime).total_seconds() / 60 > sync_failover_interval_mins:
                        log_message("debug", f"Sync failover interval exceeded for {group_name} current time: {current_time} last_sync_datetime: {last_sync_datetime}, proceeding with sync.")
                    else:
                        return

                last_successful_timestamp = successful_runs[workflow_id] if successful_runs[workflow_id] else (last_sync_datetime + timedelta(minutes=sync_failover_interval_mins))
                log_message("debug", f"Using last successful run timestamp: {last_successful_timestamp}")

                # Step 4: Update status to 'In Progress' after workflow check and before deep cloning
                update_metadata_status(metadata_table_path, workspace_name, group_name, "In Progress")
                log_message("debug", f"Set 'In Progress' status for {workspace_name}/{group_name}")

                # Step 5: Perform deep clone for each table, using the highest version before last_successful_timestamp
                for catalog, schema, table_name in parsed_tables:
                    source_table = f"{catalog}.{schema}.{table_name}"
                    version_before_timestamp = (spark.sql(f"DESCRIBE HISTORY {source_table}")
                        .filter(f"timestamp <= '{last_successful_timestamp}'")
                        .orderBy("timestamp", ascending=False)
                        .select("version", "timestamp")
                        .limit(1)
                        .collect())

                    if not version_before_timestamp:
                        log_message("info", f"No valid version found before {last_successful_timestamp} for table {source_table}. Skipping.")
                        continue

                    latest_version_before_timestamp = version_before_timestamp[0]['version']

                    # Run the deep clone SQL command with VERSION AS OF for each table
                    target_table_path = f"{target_location}/{catalog}/{schema}/{table_name}"

                    sql_command = f"""
                        CREATE OR REPLACE TABLE delta.`{target_table_path}` 
                        DEEP CLONE {source_table} VERSION AS OF {latest_version_before_timestamp}
                    """
                    log_message("debug", f"Executing SQL command: {sql_command}")
                    spark.sql(sql_command)

                    log_message("debug", f"Deep cloned table {source_table} to {target_table_path} as of version {latest_version_before_timestamp}")

        # Step 4: If there is no workflow_id, proceed directly with deep clone for the tables
        else:
            # Handle groups without workflows, perform deep clone for each table
            proceed = False
            current_time = datetime.utcnow().replace(tzinfo=pytz.UTC)
            if last_sync_time is not None:
                last_sync_datetime = last_sync_time if isinstance(last_sync_time, datetime) else datetime.strptime(last_sync_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
                if (current_time - last_sync_datetime).total_seconds() / 60 > sync_interval_mins:
                    proceed = True
            else:
                proceed = True        
            # Perform sync only if current timestamp - last_sync_time > sync_interval_mins
            if proceed:
                for catalog, schema, table_name in parsed_tables:
                    source_table = f"{catalog}.{schema}.{table_name}"
                    target_table_path = f"{target_location}/{catalog}/{schema}/{table_name}"

                    # Perform deep clone only if table does not exist
                    sql_command = f"""
                        CREATE OR REPLACE TABLE delta.`{target_table_path}` 
                        DEEP CLONE {source_table}
                    """
                    log_message("debug", f"Executing SQL command: {sql_command}")
                    spark.sql(sql_command)

                    log_message("debug", f"Deep cloned table {source_table} to {target_table_path}")
                
                last_successful_timestamp = current_time
            else:
                log_message("debug", f"Sync interval not met for group {group_name}, skipping.")
                return

        # Step 5: After successful cloning, update metadata to 'Success' and set sync time
        sync_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        update_metadata_status(metadata_table_path, workspace_name, group_name, "Success", last_successful_timestamp, sync_time)
        log_message("debug", f"Set 'Success' status for {workspace_name}/{group_name}")

        # Log summary info
        successful_workflows.append({
            'workspace_name': workspace_name,
            'group_name': group_name,
            'workflow_name': metadata['workflow_name'],
            'workflow_id': metadata['workflow_id'],
            'tables': metadata['tables'],
            'last_successful_run_time': last_successful_timestamp,
            'sync_status': 'Success'
        })
    
    except Exception as e:
        log_message("error", f"Error processing workflow {workspace_name}/{group_name}: {e}")



def sync_to_region(validated_config: dict, sync_location_to: str = "primary", max_workers: int = 10):
    """
    Syncs Delta tables from metadata with multi-threading and two levels of logging: debug and info.

    Args:
    - validated_config (dict): The validated configuration containing workspace and sync details.
    - sync_location_to (str): Specify 'primary' or 'secondary' to choose the sync location.
    - sync_interval_mins (int): Number of mins before rechecking groups without workflows.
    - sync_failover_interval_mins (int): Number of mins after which a table would be synced irrespective of workflow complition.
    - max_workers (int): Max threads for parallel processing.
    - logging_level (str): Either 'info' for summary or 'debug' for detailed logging.

    Returns:
    - None
    """

    global successful_workflows
    successful_workflows = []  # Reset the global successful workflow list

    # Validate sync_location_to
    if sync_location_to not in ["primary", "secondary"]:
        raise ValueError("sync_location_to must be either 'primary' or 'secondary'")

    try:
        # Step 1: Set up a thread pool executor to handle the workflows concurrently
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []

            # Step 2: Iterate through the workspaces in the validated config
            for workspace in validated_config['workspaces']:
                workspace_name = workspace['workspace_name']

                # Retrieve the actual PAT token using the scope and key
                secret_scope = validated_config.get("secret_scope")
                pat_key = workspace['workspace_pat_key']
                sync_failover_interval_mins = workspace['sync_failover_interval_mins']
                sync_interval_mins = workspace['sync_failover_interval_mins']
                pat_token = retrieve_pat_token(secret_scope, pat_key)
                if pat_token is None:
                    log_message("info", f"Error retrieving PAT token for workspace: {workspace_name}, skipping sync.")
                    continue

                # Choose the metadata table path based on the sync_location_to
                sync_location = workspace[f"sync_location_{sync_location_to}"]
                metadata_table_path = f"{sync_location}/meta/sync_metadata_table"
                log_message("debug", f"metadata table path {metadata_table_path}")
                log_message("info", f"Starting sync for workspace: {workspace_name} using {sync_location_to} sync location.")

                # Step 3: Load the metadata table from the selected sync location
                metadata_dict = load_metadata_table(metadata_table_path)

                # Step 4: Iterate through the metadata and submit each workflow for concurrent processing
                for workflow_data in metadata_dict.items():
                    group_name = workflow_data[0][1]  # Extract the group_name from the workflow_data

                    # Extract instance_url from the validated config
                    instance_url = workspace['workspace_instance_url']

                    # Submit each workflow for concurrent processing
                    future = executor.submit(process_workflow_to_region, workflow_data, instance_url, pat_token, sync_location, metadata_table_path, sync_interval_mins, sync_failover_interval_mins)
                    futures.append(future)

            # Step 5: Wait for all futures to complete
            for future in as_completed(futures):
                try:
                    future.result()  # Get the result from the future
                except Exception as e:
                    log_message("info", f"Error processing workflow: {e}")

        # Step 6: Display summary in info mode
        summary_df = pd.DataFrame(successful_workflows)
        summary_df = summary_df.drop(columns=['validated'], errors='ignore')  # Dropping 'validated' if exists
        log_message("info", "Summary of successfully synced workflows:")
        log_message("info", summary_df.to_string(index=False))  # Print the dataframe without index
        log_message("info", "Sync completed successfully.")

    except Exception as e:
        log_message("error", f"Error during sync process: {e}")
