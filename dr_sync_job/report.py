# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# Define variables
timezone = 'America/Chicago'
table_location = 'abfss://dr-sync@westus2drstorageacct.dfs.core.windows.net/meta/sync_metadata_table'

# SQL query wrapped in PySpark
query = f"""
SELECT 
    t3.workspace_name, 
    t3.group_name, 
    t3.workflow_name, 
    t3.source_table AS table_name,  -- Use the exploded table name
    t3.last_successful_run_time,
    t3.sync_reason,
    t3.sync_time_to_region,
    t3.sync_time_in_region,
    t1.sync_commit_version AS source_sync_commit_version,  -- Source operation metrics
    t1.source_table_size AS source_table_size,
    t2.sync_commit_version AS target_sync_commit_version,  -- Target operation metrics
    t2.source_table_size AS target_table_size,
    
    -- Adjust hour calculation to use the same timezone for both timestamps
    round(
    (unix_timestamp(from_utc_timestamp(current_timestamp(), '{timezone}')) - unix_timestamp(t3.last_successful_run_time)) / 3600, 2
    ) AS hours_since_last_sync,

    -- Sync status logic based on workflow_name presence and metric comparison
    CASE 
        WHEN t3.workflow_name IS NULL OR t3.workflow_name = '' THEN 
            CASE 
                WHEN t1.source_table_size = t2.source_table_size THEN 'Synced'
                ELSE 'Not Synced'
            END
        ELSE
            CASE
                WHEN t1.sync_commit_version = t2.sync_commit_version AND t1.source_table_size = t2.source_table_size THEN 'Synced'
                ELSE 'Not Synced'
            END
    END AS sync_status

FROM
    -- First table for source operation metrics
    (
        SELECT 
            key AS table_name,
            metric['metrics']['sync_commit_version'] AS sync_commit_version,
            metric['metrics']['source_table_size'] AS source_table_size
        FROM 
        (
            SELECT explode(from_json(source_operation_metrics, 'array<map<string, struct<metrics:map<string, long>>> >')) AS metrics_entry 
            FROM delta.`{table_location}`
            WHERE source_operation_metrics IS NOT NULL
        ) tab
        LATERAL VIEW explode(metrics_entry) AS key, metric
    ) t1
-- Join with the second table for target operation metrics
LEFT JOIN 
    (
        SELECT 
            key AS table_name,
            CAST(metric['metrics']['sync_commit_version'] AS long) AS sync_commit_version,
            CAST(metric['metrics']['source_table_size'] AS long) AS source_table_size
        FROM 
        (
            SELECT explode(from_json(target_operation_metrics, 'array<map<string, struct<metrics:map<string, string>>> >')) AS metrics_entry 
            FROM delta.`{table_location}`
            WHERE target_operation_metrics IS NOT NULL
        ) tab
        LATERAL VIEW explode(metrics_entry) AS key, metric
    ) t2
ON t1.table_name = t2.table_name
-- Join with the third table for general info
LEFT JOIN 
    (
        SELECT 
            workspace_name, 
            group_name, 
            workflow_name, 
            explode(tables) AS source_table,  -- Explode the tables array
            from_utc_timestamp(from_unixtime(last_successful_run_time / 1000), '{timezone}') AS last_successful_run_time,
            sync_reason,
            from_utc_timestamp(from_unixtime(sync_time_to_region / 1000), '{timezone}') AS sync_time_to_region,
            from_utc_timestamp(from_unixtime(sync_time_in_region / 1000), '{timezone}') AS sync_time_in_region
        FROM delta.`{table_location}`
    ) t3
ON t1.table_name = t3.source_table
"""

# Run the query using PySpark
display(spark.sql(query))
