# Databricks notebook source
import time
from pyspark.sql import functions as F

def sync_in_region(validated_config: dict, sync_location_from: str = "secondary", max_workers: int = 10):
    """
    Syncs Delta tables from an external location (abfss path) into Unity Catalog based on the sync times.

    Args:
    - validated_config (dict): The validated configuration containing workspace and sync details.
    - sync_location_from (str): Specify 'primary' or 'secondary' to choose the sync location to read from.
    - max_workers (int): The number of workers for multithreading.

    Returns:
    - None
    """
    try:
        # Step 1: Iterate through the workspaces in the validated config
        for workspace in validated_config['workspaces']:
            workspace_name = workspace['workspace_name']

            # Choose the metadata table path based on the sync_location_from
            sync_location = workspace[f"sync_location_{sync_location_from}"]
            metadata_table_path = f"{sync_location}/meta/sync_metadata_table"

            log_message("info", f"Starting sync for workspace: {workspace_name} using {sync_location_from} sync location.")

            # Step 2: Load the metadata table into a Spark DataFrame
            metadata_df = spark.read.format("delta").load(metadata_table_path)

            # Step 3: Filter records where sync_time_to_region >= sync_time_in_region or sync_time_in_region is null (first-time sync)
            filtered_metadata_df = metadata_df.filter(
                (metadata_df["sync_time_to_region"].isNotNull()) &
                (
                    metadata_df["sync_time_in_region"].isNull() |  # Include first-time syncs where sync_time_in_region is null
                    (metadata_df["sync_time_to_region"] >= metadata_df["sync_time_in_region"])  # Or where sync_time_to_region >= sync_time_in_region
                )
            )

            if filtered_metadata_df.count() == 0:
                log_message("info", "No records to sync.")
                continue

            # Step 4: Capture the current timestamp for sync_time_in_region
            sync_time_in_region = int(time.time() * 1000)

            # Step 5: Collect filtered rows and sync them in parallel
            records_to_sync = filtered_metadata_df.collect()  # Collect the filtered records to sync

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []

                for row in records_to_sync:
                    workspace_name = row["workspace_name"]
                    group_name = row["group_name"]
                    metadata = {
                        "tables": row["tables"],
                        "sync_time_to_region": row["sync_time_to_region"],
                        "sync_time_in_region": row["sync_time_in_region"]
                    }
                    # Submit each group to a worker thread
                    future = executor.submit(
                        process_sync_to_region,
                        workspace_name,
                        group_name,
                        metadata,
                        sync_location, 
                        metadata_table_path,
                        sync_time_in_region
                    )
                    futures.append(future)

                # Wait for all threads to complete
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        log_message("error", f"Error during sync: {e}")

        log_message("info", "Sync completed successfully.")
    
    except Exception as e:
        log_message("error", f"Failed to sync tables: {e}")


def process_sync_to_region(workspace_name: str, group_name: str, metadata: dict, sync_location: str, metadata_table_path: str, sync_time_in_region: str):
    """
    Processes the sync for each group, reading from abfss location and deep cloning into Unity Catalog.

    Args:
    - workspace_name (str): The workspace name.
    - group_name (str): The group name.
    - metadata (dict): The metadata record for the group.
    - sync_location (str): The sync location (e.g., secondary).
    - metadata_table_path (str): The path to the metadata table for updating.
    - sync_time_in_region (str): The timestamp to update sync_time_in_region.

    Returns:
    - None
    """
    try:
        # Step 1: Extract tables from the metadata
        tables = metadata["tables"]
        sync_time_to_region = metadata.get("sync_time_to_region")  # Get the sync_time_to_region for comparison

        if not sync_time_to_region:
            log_message("info", f"No sync_time_to_region found for {workspace_name}/{group_name}. Skipping.")
            return

        for table in tables:
            # Parse the catalog, schema, and table name from the table string
            catalog, schema, table_name = table.split(".")
            
            # Step 2: Create the abfss path based on the external location (sync_location)
            abfss_path = f"{sync_location}/{catalog}/{schema}/{table_name}"

            # Step 3: Get the highest version before sync_time_to_region using 'DESCRIBE HISTORY'
            history_query = f"DESCRIBE HISTORY delta.`{abfss_path}`"
            history_df = spark.sql(history_query)

            # Filter the history to get the version before sync_time_to_region
            version_before_timestamp = (history_df
                                        .filter(
                                            F.col("timestamp") <= F.from_unixtime(F.lit(sync_time_to_region) / 1000).cast("timestamp")
                                         )
                                        .orderBy("timestamp", ascending=False)
                                        .select("version", "timestamp")
                                        .limit(1)
                                        .collect())

            if not version_before_timestamp:
                log_message("info", f"No valid version found before {sync_time_to_region} for {abfss_path}. Skipping.")
                return

            latest_version_before_sync_time = version_before_timestamp[0]['version']

            # Step 4: Perform deep clone using the version identified
            target_table = f"{catalog}.{schema}.{table_name}"

            sql_command = f"""
                CREATE TABLE IF NOT EXISTS {target_table} 
                DEEP CLONE delta.`{abfss_path}` VERSION AS OF {latest_version_before_sync_time}
            """
            log_message("debug", f"Executing SQL: {sql_command}")
            spark.sql(sql_command)

            log_message("info", f"Table {table} deep cloned into Unity Catalog at {target_table} using version {latest_version_before_sync_time}")

        # Step 5: Update the metadata with the sync_time_in_region
        update_metadata_status(
            metadata_table_path, 
            workspace_name, 
            group_name, 
            sync_time_in_region=sync_time_in_region
        )
        log_message("debug", f"Updated sync_time_in_region for {workspace_name}/{group_name}")

    except Exception as e:
        log_message("error", f"Error syncing {workspace_name}/{group_name}: {e}")
