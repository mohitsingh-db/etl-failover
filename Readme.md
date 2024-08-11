# Databricks Workspace Disaster Recovery Sync Job

## Overview

This document describes the configuration and operation of a sync job that runs in a Databricks workspace. The primary purpose of this job is to maintain a synchronized copy of critical data in a secondary region, ensuring data availability in the event of a disaster. The job is designed to run at scheduled intervals, adhering to defined RTO (Recovery Time Objective) and RPO (Recovery Point Objective) requirements.

## Key Features

- **Disaster Recovery**: Ensures data is synchronized between primary and secondary regions.
- **Customizable Scheduling**: Supports flexible scheduling based on organizational RTO and RPO needs, with a minimum schedule interval of 30 minutes.
- **Error Handling and Notifications**: Automatically sends notifications on job success or failure, with configurable retry mechanisms.
- **Metadata Management**: Maintains comprehensive metadata records for audit and operational continuity.

## Configuration Details

### Sync Job Configuration

The sync job is configured at the workspace level, allowing users to define groups of objects (e.g., tables) that need to be synchronized. Each sync group can be associated with specific workflows and tables.

```json
{ "secret_scope" : "",
  "workspaces": [
    {
      "workspace_name": "dr_test_east_workspace",
      "workspace_instance_url": "https://<workspace-1>.databricks.com",
      "workspace_pat_key" : "", 
      "sync_location_primary": "abfss://container@storageaccount.dfs.core.windows.net/",
      "sync_location_secondary": "abfss://container@storageaccount.dfs.core.windows.net/",
      "sync_failure_notification": "mohit.singh@databricks.com",
      "sync_success_notification": "mohit.singh@databricks.com",
      "sync_default_schedule": "0 */12 * * *",
      "sync_max_retries": "3",
      "sync_groups": [
        {
          "group_name": "core_product",
          "group_owner_email": "mohit.singh@databricks.com",
          "workflow": "core_product_job",
          "tables": ["core_raw", "core_silver", "core_gold", "job_metadata"]
        },
        {
          "group_name": "example_group_1",
          "group_owner_email": "example1@example.com",
          "workflow": "example_workflow_1",
          "tables": ["example_table_1"]
        },
        {
          "group_name": "scheduled_sync",
          "group_owner_email": "example2@example.com",
          "tables": ["table_A", "table_B", "table_C"]
        }
      ]
    }
  ]
}
```

## Configuration Loading

### Loading Configuration

The JSON configuration file is loaded from a specified directory within the Databricks workspace. During each scheduled sync, the job reads this configuration file to identify the workspaces, sync groups, workflows, and tables that need to be synchronized. This approach ensures that any new changes to the configuration will be captured and applied in the next run, maintaining up-to-date sync operations.

### Understanding the Configuration Parameters

- **workspace_name**: The name of the Databricks workspace where the sync objects (such as tables and workflows) are located. This name helps identify which workspace's data will be involved in the synchronization process.

- **workspace_instance_url**: The URL of the Databricks workspace where the sync objects reside. This URL is used by the sync job to access the workspace and perform the necessary operations.

- **sync_location**: An intermediate storage path for the sync job in the secondary region. This path must be added as an external location in the primary region's metastore, and the principal executing the sync job must have write access to this external location. It serves as the destination for deep cloning and syncing the data.

- **Notification**: 
  - **Sync Success Notification**: An email notification sent to users upon the successful completion of the sync job.
  - **Sync Failure Notification**: An email notification sent to users if the sync job encounters any errors, allowing for timely intervention.

- **sync_default_schedule**: This parameter defines the schedule for syncing tables, independent of workflow completion. It specifies the regular interval at which tables should be synchronized, ensuring consistent data updates even if workflows are not triggered.

- **sync_groups**: A collection of objects, such as workflows and tables, that are grouped together for synchronization. The state of these objects is synchronized as an atomic unit, ensuring that all changes within the group are applied consistently across regions.

# -- validation
  # First do basic validation of this json, for example group_name should be unique. tables and sync_default_schedule etc. only workflow is optional.
  # Then loop over these entries and Validate if workflow exist in databricks workspace, also validate if the tables exists in the Unity catalog, do we have read permission on these tables.
  # then validate if the target sync location is available in the parameters and this job has access to it.
  # if they don not exists the just print the error message. but continue the job. In next run of this job we will again check for the failure one.
# -- metadata creation.
  # The job will create two set of metadata tables. One extrenal table in primary region and second in secondary region.
  # The schema of these tables are as following 
  # group_name, workflow, tables, date_created, date_updated, sync_start_timestamp, sync_end_timestamp, current_sync_status, message.
  # first check if table exists in both location. if not then create it.
  # If it do exists in one location then deep clone it into the other location.
  # if they exists then merge these config into target table, with only these column updates. keep the sync_start_timestamp, current_sync_status untouched.
# -- processing start.
  # if for some reason the secondary region location is not available then keep skip the part and update status as failed. withe message secondary region not available.
  # Loop through each entry in metadata table and check if the dependent workflow has completed since last run.
    # If last run do not have any information. the read the workflow last end timestamp and it should be successful.
    # check if there is a successful run for this workflow, since last check.
    # keep the sync_start_timestamp i.e current timestamp in memeory.
    # if yes then update  current_sync_status to in-progress.
      # Read the tables as of version for that workflow for commits last latest then this timestamp and deep clone into target table.
    # once the run is completed then update the sync_start_timestamp and current_sync_status to success.  
    # if the sync fail becuase of any issue then update the current_sync_status to failed and keep the old timestamp.
    # these updates will be done in both metadata tables in both region.

    # job will keep checking if any workflow is running in secondary region while dr is in passive mode.
    
    

