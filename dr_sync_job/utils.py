# Databricks notebook source
import requests
import pytz
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, LongType
from pyspark.sql import DataFrame
import traceback
import time
import random
from pyspark.sql.functions import lit, col, unix_timestamp


# Define the schema of the metadata table, including new columns
schema = StructType([
    StructField("workspace_name", StringType(), False),
    StructField("group_name", StringType(), False),
    StructField("workflow_name", StringType(), True),
    StructField("workflow_id", StringType(), True),
    StructField("tables", ArrayType(StringType()), False),
    StructField("validated", BooleanType(), False),
    StructField("last_successful_run_time", LongType(), True),
    StructField("sync_status", StringType(), True),
    StructField("sync_reason", StringType(), True),
    StructField("sync_time_to_region", LongType(), True), 
    StructField("sync_time_in_region", LongType(), True),
    StructField("source_operation_metrics", StringType(), True),
    StructField("target_operation_metrics", StringType(), True)
])

def log_message(level: str, message: str):
    """
    Logs a message based on the current global log level.

    Args:
    - level (str): The level of the message ('debug', 'info', or 'error').
    - message (str): The message to be logged.
    """
    global LOG_LEVEL
    levels = {"debug": 1, "info": 2, "error": 3}  # Logging levels priority

    # Log the message only if the level is equal to or higher than the current log level
    if levels.get(level, 0) >= levels.get(LOG_LEVEL, 0):
        print(f"{level.upper()}: {message}")

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
    
def update_metadata_status(metadata_table_path: str, workspace_name: str, group_name: str, 
                           status: str = None, sync_time: int = None, sync_time_to_region: int = None, 
                           sync_time_in_region: int = None, sync_reason: str = None, 
                           source_operation_metrics: dict = None, target_operation_metrics: dict = None):
    """
    Updates the metadata table with the current status, sync timestamps, and metrics for the regions.

    Args:
    - metadata_table_path (str): Path to the metadata Delta table.
    - workspace_name (str): The workspace name of the group.
    - group_name (str): The group name.
    - status (str, optional): The sync status ('In Progress' or 'Success').
    - sync_time (int, optional): The last sync time in epoch format.
    - sync_reason (str, optional): The reason for the sync.
    - sync_time_to_region (int, optional): The sync timestamp for the "to" region in epoch format.
    - sync_time_in_region (int, optional): The sync timestamp for the "in" region in epoch format.
    - source_operation_metrics (dict, optional): Metrics related to the source operations.
    - target_operation_metrics (dict, optional): Metrics related to the target operations.

    Returns:
    - None
    """
    try:
        # Start constructing the SQL query
        update_query = f"UPDATE delta.`{metadata_table_path}` SET "
        
        # List to hold column update expressions
        update_columns = []
        
        # Add non-None values to the update query
        if status is not None:
            update_columns.append(f"sync_status = '{status}'")
        if sync_time is not None:
            update_columns.append(f"last_successful_run_time = {sync_time}")
        if sync_time_to_region is not None:
            update_columns.append(f"sync_time_to_region = {sync_time_to_region}")
        if sync_time_in_region is not None:
            update_columns.append(f"sync_time_in_region = {sync_time_in_region}")
        if sync_reason is not None:
            update_columns.append(f"sync_reason = '{sync_reason}'")
        
        # Handle source_operation_metrics, converting to JSON string if provided
        if source_operation_metrics is not None:
            source_metrics_json = json.dumps(source_operation_metrics)
            update_columns.append(f"source_operation_metrics = '{source_metrics_json}'")
        
        # Handle target_operation_metrics, converting to JSON string if provided
        if target_operation_metrics is not None:
            target_metrics_json = json.dumps(target_operation_metrics)
            update_columns.append(f"target_operation_metrics = '{target_metrics_json}'")
        
        # Join all update columns to form the SET part of the SQL query
        update_query += ", ".join(update_columns)
        
        # Add WHERE clause to filter by workspace_name and group_name
        update_query += f" WHERE workspace_name = '{workspace_name}' AND group_name = '{group_name}'"
        
        # Log and execute the update query
        log_message("debug", f"Executing SQL update: {update_query}")
        execute_with_retry(update_query)
        
        log_message("debug", f"Metadata updated for {workspace_name}/{group_name} with the following: {update_columns}")
    
    except Exception as e:
        log_message("error", f"Error updating metadata: {str(e)}")
        raise

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
            log_message("debug",f"Access check successful at path: {path}")

            # Clean up by deleting the temporary file
            dbutils.fs.rm(temp_file_path)
            return True
        else:
            log_message("debug",f"Could not confirm the creation of the file at path: {path}")
            return False
    except Exception as e:
        log_message("error",f"Access check failed at path: {path}. Error: {e}")
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
            "sync_status": <sync_status>,
            "sync_time_to_region": <sync_time_to_region>,
            "sync_time_in_region": <sync_time_in_region>
        }
      }
    """
    try:
        # Check if the path exists
        dbutils.fs.ls(path)
    except Exception as e:
        log_message("debug", f"Path does not exist: {path}. Creating a new metadata table.")
        create_empty_metadata_table(path)
        return {}
    
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
            sync_time_to_region = row['sync_time_to_region']
            sync_time_in_region = row['sync_time_in_region']

            # Add the metadata to the dictionary with formatted timestamps
            metadata_dict[(workspace_name, group_name)] = {
                "workflow_name": workflow_name,
                "workflow_id": workflow_id,
                "tables": tables,
                "validated": validated,
                "last_successful_run_time": last_successful_run_time,
                "sync_status": sync_status,
                "sync_time_to_region": sync_time_to_region,
                "sync_time_in_region": sync_time_in_region
            }

        log_message("debug",f"Metadata table loaded successfully from: {path}")
        return metadata_dict

    except Exception as e:
        log_message("debug",f"Error loading metadata table from path: {path}. Error: {e}")
        return {}

def create_empty_metadata_table(path: str):
    """
    Creates an empty metadata Delta table at the specified path.

    Args:
    - path (str): The path where the Delta table will be created.
    """
    
    # Create an empty DataFrame with the defined schema
    empty_df = spark.createDataFrame([], schema=schema)

    # Write the empty DataFrame as a Delta table
    empty_df.write.format("delta").mode("overwrite").save(path)
    log_message("debug",f"Empty metadata table created at: {path}")

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
        log_message("error",f"Error fetching job info for workflows '{workflow_names}': {e}")
        return {}

def job_successful_run_after_last_run(instance_url: str, pat_token: str, job_ids: list, last_run_timestamp: int = None) -> dict:
    """
    Checks if any of the given Databricks jobs have had successful runs after the last recorded run timestamp.

    Args:
    - instance_url (str): The URL of the Databricks instance.
    - pat_token (str): The PAT token for authentication.
    - job_ids (list): A list of job IDs to check.
    - last_run_timestamp (int, optional): The timestamp of the last recorded run in epoch time (milliseconds). 
      If None, the function will return the latest successful run without comparison.

    Returns:
    - dict: A dictionary where the keys are job IDs and the values are the last successful run end timestamps in 
      epoch format (milliseconds). If no successful run is found after the given timestamp, the value will be None.
    """
    try:
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
                "limit": 25, 
                "start_time_from": last_run_timestamp
            }

            # Make the request to the Databricks Jobs API to list job runs
            response = requests.get(endpoint, headers=headers, params=params)
            response.raise_for_status()  # Raise an exception for HTTP errors

            # Parse the JSON response
            runs = response.json().get("runs", [])

            # Variable to store the latest successful run end time after last_run_timestamp
            last_successful_run_end_time = None

            # Iterate over the runs and find the latest successful run after the last run
            for run in runs:
                run_end_time = run.get("end_time", 0)  # Timestamp in milliseconds
                run_result_state = run.get("state", {}).get("result_state")

                if run_result_state == "SUCCESS":
                    if last_run_timestamp is None:
                        # If no last run timestamp is provided, track the latest successful run
                        if last_successful_run_end_time is None or run_end_time > last_successful_run_end_time:
                            last_successful_run_end_time = run_end_time
                    elif run_end_time > last_run_timestamp:
                        # If the run is after the last known run timestamp, track the latest successful run
                        if last_successful_run_end_time is None or run_end_time > last_successful_run_end_time:
                            last_successful_run_end_time = run_end_time

            # Add the result to the dictionary
            successful_runs[job_id] = last_successful_run_end_time

        return successful_runs

    except requests.exceptions.RequestException as e:
        log_message("error", f"Error checking job runs: {e}")
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
        log_message("debug",f"Successfully retrieved PAT token for secret scope: {secret_scope}, key: {pat_key}")
        return pat_token
    except Exception as e:
        log_message("error",f"Error retrieving PAT token from secret scope: {secret_scope}, key: {pat_key}. Error: {e}")
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
            log_message("debug",f"Read permission confirmed for table {table}.")
            return True
        else:
            log_message("debug",f"Table '{table}' does not exist.")
            return False
    except Exception as e:
        log_message("error",f"Error checking read permission for table '{table}': {e}")
        return False
    
def merge_metadata_entries(metadata_table_path: str, new_entries: list) -> None:
    """
    Merge new entries into the Delta metadata table using the MERGE INTO SQL command.
    Insert new entries, update existing entries only when specific columns differ, and delete entries that no longer exist in the source.

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
    try:
        # Convert the new entries list into a DataFrame
        new_entries_df = spark.createDataFrame(new_entries, schema=schema)
        # Step 1: Create a temporary view from new_entries
        new_entries_df.createOrReplaceTempView("new_metadata")

        # Step 2: Use the MERGE INTO command to merge the new entries into the Delta table
        merge_sql = f"""
            MERGE INTO delta.`{metadata_table_path}` AS target
            USING new_metadata AS source
            ON target.workspace_name = source.workspace_name
            AND target.group_name = source.group_name
            WHEN MATCHED AND 
                (target.workflow_name != source.workflow_name
                OR target.tables != source.tables
                OR target.validated != source.validated) 
            THEN 
                UPDATE SET 
                    target.workflow_name = source.workflow_name,
                    target.tables = source.tables,
                    target.validated = source.validated,
                    target.last_successful_run_time = NULL,
                    target.sync_status = NULL,
                    target.sync_reason = NULL,
                    target.sync_time_to_region = NULL,
                    target.sync_time_in_region = NULL,
                    target.source_operation_metrics = NULL,
                    target.target_operation_metrics = NULL
            WHEN NOT MATCHED THEN
                INSERT (
                    workspace_name, 
                    group_name, 
                    workflow_name, 
                    workflow_id, 
                    tables, 
                    validated, 
                    last_successful_run_time, 
                    sync_status,
                    sync_reason,
                    sync_time_to_region,
                    sync_time_in_region,
                    source_operation_metrics,
                    target_operation_metrics
                ) VALUES (
                    source.workspace_name, 
                    source.group_name, 
                    source.workflow_name, 
                    source.workflow_id, 
                    source.tables, 
                    source.validated, 
                    NULL, 
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL
                )
            WHEN NOT MATCHED BY SOURCE THEN
                DELETE
        """

        # Step 3: Execute the merge SQL
        execute_with_retry(merge_sql)

        log_message("debug",f"Metadata table merged successfully at: {metadata_table_path}")

    except Exception as e:
        error_details = traceback.format_exc()
        log_message("error", f"Error during metadata merge: {error_details}")

def execute_with_retry(sql_query: str, max_retries: int = 3):
    """
    Executes a given SQL query with retry logic in case of failure.

    Args:
    - sql_query (str): The SQL query to be executed.
    - max_retries (int): The maximum number of retry attempts (default is 3).

    Returns:
    - None
    """
    for attempt in range(max_retries):
        try:
            spark.sql(sql_query)
            break  # Exit the loop if the query is successful

        except Exception as e:
            log_message("error", f"Error detected ({type(e).__name__}). Attempt {attempt + 1} of {max_retries}.")
            
            if attempt < max_retries - 1:
                # Exponential backoff with jitter (randomness)
                sleep_time = (10 * (attempt + 1)) + random.uniform(0, 2)
                log_message("debug", f"Retrying after {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                log_message("error", f"Failed to execute SQL query after {max_retries} attempts.")
                raise