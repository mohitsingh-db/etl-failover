# Databricks notebook source
import requests
import pytz
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, TimestampType
from pyspark.sql import DataFrame


def load_config(file_path: str) -> dict:
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config

def find_workspace_config(workspace_name: str, validated_config: dict) -> dict:
    """
    Finds and returns the corresponding workspace configuration from the validated config.

    Args:
    - workspace_name (str): The name of the workspace.
    - validated_config (dict): The validated configuration containing workspace details.

    Returns:
    - dict: The workspace configuration if found, otherwise None.
    """
    for workspace in validated_config['workspaces']:
        if workspace['workspace_name'] == workspace_name:
            return workspace
    return None
    
def update_metadata_status(metadata_table_path, workspace_name, group_name, status, sync_time=None):
    """
    Updates the metadata table with the current status and sync timestamp.

    Args:
    - metadata_table_path (str): Path to the metadata Delta table.
    - workspace_name (str): The workspace name of the group.
    - group_name (str): The group name.
    - status (str): The sync status ('In Progress' or 'Success').
    - sync_time (str, optional): The last sync time, updated upon success.

    Returns:
    - None
    """
    try:
        # Load the current metadata table
        metadata_df = spark.read.format("delta").load(metadata_table_path)
        
        # Update the status and timestamp for the specific workspace and group
        updated_metadata_df = metadata_df.withColumn(
            "sync_status",
            F.when(
                (metadata_df["workspace_name"] == workspace_name) &
                (metadata_df["group_name"] == group_name),
                F.lit(status)
            ).otherwise(metadata_df["sync_status"])
        )
        
        if sync_time:
            updated_metadata_df = updated_metadata_df.withColumn(
                "last_successful_run_time",
                F.when(
                    (metadata_df["workspace_name"] == workspace_name) &
                    (metadata_df["group_name"] == group_name),
                    F.lit(sync_time)
                ).otherwise(metadata_df["last_successful_run_time"])
            )
        
        # Overwrite the metadata table with the updated data
        updated_metadata_df.write.format("delta").mode("overwrite").save(metadata_table_path)

        log_message("debug", f"Metadata updated for {workspace_name}/{group_name}: Status = {status}, Sync Time = {sync_time}")
    
    except Exception as e:
        log_message("info", f"Failed to update metadata for {workspace_name}/{group_name}: {e}")

def check_fs_path(path: str) -> bool:
    """
    Check if the program has access to create and delete a file at the given storage path.

    Args:
    - path (str): The storage path to check.

    Returns:
    - bool: True if the program has write access, False otherwise.
    """
    try:
        # Generate a temporary file name
        temp_file_path = f"{path}/temp_access_check.txt"

        # Write a temporary file to the storage path
        dbutils.fs.put(temp_file_path, "test", overwrite=True)

        # Confirm the file was written by checking its existence
        if dbutils.fs.ls(path):
            print(f"Access check successful at path: {path}")

            # Clean up by deleting the temporary file
            dbutils.fs.rm(temp_file_path)
            return True
        else:
            print(f"Error: Could not confirm the creation of the file at path: {path}")
            return False
    except Exception as e:
        print(f"Access check failed at path: {path}. Error: {e}")
        return False

def load_metadata_table(path: str) -> dict:
    """
    Load the metadata table from the given path and convert it into a dictionary.

    Args:
    - path (str): The path where the metadata table is stored.

    Returns:
    - dict: The metadata table as a dictionary, structured as:
      {
        (workspace_name, group_name): {
            "workflow_name": <workflow_name>,
            "workflow_id": <workflow_id>,
            "tables": [<table1>, <table2>, ...],
            "validated": <validated_status>,
            "last_successful_run_time": <last_successful_run_time>,
            "sync_status": <sync_status>
        }
      }
    """
    try:
        # Load the Delta table into a DataFrame
        df = spark.read.format("delta").load(path)

        # Initialize an empty dictionary to hold the metadata
        metadata_dict = {}

        # Iterate over the DataFrame rows and populate the dictionary
        for row in df.collect():
            workspace_name = row['workspace_name']
            group_name = row['group_name']
            workflow_name = row['workflow_name']
            workflow_id = row['workflow_id']
            tables = row['tables']
            validated = row['validated']
            last_successful_run_time = row['last_successful_run_time']
            sync_status = row['sync_status']

            # Add the metadata to the dictionary
            metadata_dict[(workspace_name, group_name)] = {
                "workflow_name": workflow_name,
                "workflow_id": workflow_id,
                "tables": tables,
                "validated": validated,
                "last_successful_run_time": last_successful_run_time,
                "sync_status": sync_status
            }

        print(f"Metadata table loaded successfully from: {path}")
        return metadata_dict

    except Exception as e:
        print(f"Error loading metadata table from path: {path}. Error: {e}")
        print(f"Creating a new metadata table at path: {path}")
        create_empty_metadata_table(path)
        return {}

def create_empty_metadata_table(path: str):
    """
    Creates an empty metadata Delta table at the specified path.

    Args:
    - path (str): The path where the Delta table will be created.
    """
    # Define the schema of the metadata table
    schema = StructType([
        StructField("workspace_name", StringType(), False),
        StructField("group_name", StringType(), False),
        StructField("workflow_name", StringType(), False),
        StructField("workflow_id", StringType(), False),
        StructField("tables", ArrayType(StringType()), False),
        StructField("validated", BooleanType(), False),
        StructField("last_successful_run_time", TimestampType(), True),
        StructField("sync_status", StringType(), True)
    ])

    # Create an empty DataFrame with the defined schema
    empty_df = spark.createDataFrame([], schema)

    # Write the empty DataFrame as a Delta table
    empty_df.write.format("delta").mode("overwrite").save(path)
    print(f"Empty metadata table created at: {path}")

def get_databricks_jobs_info(instance_url: str, pat_scope: str, pat_token: str, workflow_names: list) -> dict:
    """
    Retrieves job information from Databricks REST API for a list of workflows using API version 2.1 with pagination.

    Args:
    - instance_url (str): The URL of the Databricks instance.
    - pat_token (str): The PAT token for authentication.
    - workflow_names (list): A list of workflow/job names to retrieve information for.

    Returns:
    - dict: A dictionary with workflow names as keys and their corresponding job IDs as values.
    """
    try:
        pat = retrieve_pat_token(pat_scope, pat_token)
        # Construct the API endpoint URL for Jobs API 2.1
        endpoint = f"{instance_url}/api/2.1/jobs/list"

        # Set up the headers for the API request
        headers = {
            "Authorization": f"Bearer {pat}"
        }

        # Initialize variables for pagination
        jobs_info = {}
        has_more = True
        offset = 0

        # Paginate through all jobs
        while has_more and len(jobs_info) < len(workflow_names):
            # Include the offset parameter to handle pagination
            params = {
                "limit": 25,  # Adjust limit to match API requirements
                "offset": offset
            }
            response = requests.get(endpoint, headers=headers, params=params)
            response.raise_for_status()

            # Parse the JSON response
            result = response.json()
            jobs = result.get("jobs", [])

            # Search for the jobs by name and collect information
            for job in jobs:
                job_name = job.get("settings", {}).get("name")
                if job_name in workflow_names and job_name not in jobs_info:
                    jobs_info[job_name] = job.get("job_id")
                    # If we've found all requested jobs, return early
                    if len(jobs_info) == len(workflow_names):
                        return jobs_info

            # Check if there are more jobs to retrieve
            has_more = len(jobs) == 25  # Adjust to the limit used
            offset += 25  # Increment the offset for the next page

        # Return the collected job information
        return jobs_info

    except requests.exceptions.RequestException as e:
        print(f"Error fetching job info for workflows '{workflow_names}': {e}")
        return {}

def job_successful_run_after_last_run(instance_url: str, pat_token: str, job_ids: list, last_run_timestamp_str: str = None) -> dict:
    """
    Checks if any of the given Databricks jobs have had successful runs after the last recorded run timestamp.

    Args:
    - instance_url (str): The URL of the Databricks instance.
    - pat_token (str): The PAT token for authentication.
    - job_ids (list): A list of job IDs to check.
    - last_run_timestamp_str (str, optional): The timestamp of the last recorded run in UTC format "yyyy-mm-dd hh:mm:ss".
      If None, the function will return the latest successful run without comparison.

    Returns:
    - dict: A dictionary where the keys are job IDs and the values are the last successful run end timestamps.
            If no successful run is found after the given timestamp, the value will be None.
    """
    try:
        if last_run_timestamp_str is not None:
            # Convert the input timestamp string to a datetime object in UTC
            last_run_timestamp = datetime.strptime(last_run_timestamp_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=pytz.UTC)
        else:
            last_run_timestamp = None

        # Set up the headers for the API request
        headers = {
            "Authorization": f"Bearer {pat_token}"
        }

        successful_runs = {}

        # Iterate over each job_id
        for job_id in job_ids:
            # Construct the API endpoint URL for listing runs of the specific job
            endpoint = f"{instance_url}/api/2.1/jobs/runs/list"

            # Define the parameters for filtering job runs
            params = {
                "job_id": job_id,
                "completed_only": "true",
                "limit": 25  # Limit the number of runs to retrieve (adjust as needed)
            }

            # Make the request to the Databricks Jobs API to list job runs
            response = requests.get(endpoint, headers=headers, params=params)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the JSON response
            runs = response.json().get("runs", [])

            # Variable to store the last successful run end time
            last_successful_run_end_time = None

            # Iterate over the runs and check if there is a successful run after the last run
            for run in runs:
                run_end_time = run.get("end_time", 0) / 1000  # Convert from milliseconds to seconds
                run_end_datetime = datetime.fromtimestamp(run_end_time, tz=pytz.UTC)
                run_result_state = run.get("state", {}).get("result_state")

                if last_run_timestamp is None:
                    # If no last run timestamp is provided, return the latest successful run
                    if run_result_state == "SUCCESS":
                        last_successful_run_end_time = run_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                        break
                elif run_end_datetime > last_run_timestamp and run_result_state == "SUCCESS":
                    last_successful_run_end_time = run_end_datetime.strftime("%Y-%m-%d %H:%M:%S")
                    break  # Exit the loop early as we found a successful run

            # Add the result to the dictionary
            successful_runs[job_id] = last_successful_run_end_time

        return successful_runs

    except requests.exceptions.RequestException as e:
        print(f"Error checking job runs: {e}")
        return {}

def retrieve_pat_token(secret_scope: str, pat_key: str) -> str:
    """
    Retrieves the Personal Access Token (PAT) for a specific workspace from the Databricks secret store.

    Args:
    - secret_scope (str): The scope within the secret store where the PAT is stored.
    - pat_key (str): The key identifying the PAT within the secret scope.

    Returns:
    - str: The retrieved PAT token.
    """
    try:
        # Use Databricks utilities to fetch the PAT token
        pat_token = dbutils.secrets.get("dr-test-scope", key=pat_key)
        print(f"Successfully retrieved PAT token for secret scope: {secret_scope}, key: {pat_key}")
        return pat_token
    except Exception as e:
        print(f"Error retrieving PAT token from secret scope: {secret_scope}, key: {pat_key}. Error: {e}")
        return None
    
def check_table(table: str) -> bool:
    """
    Check if the current user has read permission on a table by checking if the table exists and can be described.

    Args:
    - table (str): The fully qualified name of the table (catalog.schema.table_name).

    Returns:
    - bool: True if the user has read permission, False otherwise.
    """
    try:
        # Check if the table exists using Spark catalog
        if spark.catalog.tableExists(table):
            # Try to describe the table's metadata to ensure read access
            spark.catalog.listColumns(table)
            print(f"Read permission confirmed for table {table}.")
            return True
        else:
            print(f"Table '{table}' does not exist.")
            return False
    except Exception as e:
        print(f"Error checking read permission for table '{table}': {e}")
        return False
    
def merge_metadata_entries(metadata_table_path: str, new_entries: list) -> None:
    """
    Merge new entries into the Delta metadata table.

    Args:
    - metadata_table_path (str): The Delta table path in the primary sync location.
    - new_entries (list): A list of new metadata entries, each containing:
      - workspace_name (str)
      - group_name (str)
      - workflow_name (str)
      - workflow_id (str)
      - tables (list of str)
      - validated (bool)

    Returns:
    - None
    """

    # Step 1: Load the existing metadata table from the primary location
    metadata_df = spark.read.format("delta").load(metadata_table_path)

    # Convert the DataFrame to a dictionary for easy manipulation
    metadata_dict = {
        (row['workspace_name'], row['group_name']): row.asDict()
        for row in metadata_df.collect()
    }

    # Step 2: Merge the new entries with the existing metadata
    new_entries_dict = {
        (entry['workspace_name'], entry['group_name']): entry
        for entry in new_entries
    }

    # Identify entries to delete
    keys_to_delete = set(metadata_dict.keys()) - set(new_entries_dict.keys())
    for key in keys_to_delete:
        print(f"Deleting entry for group '{key[1]}' in workspace '{key[0]}' from metadata table.")
        del metadata_dict[key]

    # Update or add new entries
    for key, new_entry in new_entries_dict.items():
        if key in metadata_dict:
            existing_entry = metadata_dict[key]
            # Update only the workflow_name, tables, and validated fields
            if (
                existing_entry['workflow_name'] != new_entry['workflow_name'] or
                set(existing_entry['tables']) != set(new_entry['tables']) or
                existing_entry['validated'] != new_entry['validated']
            ):
                print(f"Updating specific columns for group '{key[1]}' in workspace '{key[0]}'.")
                existing_entry['workflow_name'] = new_entry['workflow_name']
                existing_entry['tables'] = new_entry['tables']
                existing_entry['validated'] = new_entry['validated']
                metadata_dict[key] = existing_entry
        else:
            print(f"Adding new entry for group '{key[1]}' in workspace '{key[0]}'.")
            metadata_dict[key] = new_entry

    # Convert the updated metadata dictionary back to a DataFrame
    updated_metadata_df = spark.createDataFrame(
        data=list(metadata_dict.values()),
        schema=metadata_df.schema
    )

    # Step 3: Write the updated metadata table back to the location
    updated_metadata_df.write.format("delta").mode("overwrite").save(metadata_table_path)

    print(f"Metadata table has been merged and updated at the location: {metadata_table_path}")