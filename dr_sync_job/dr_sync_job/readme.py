# This is sync job that will run in databricks workspace to provide a copy of data in secondary region in cases of disaster.
# this will run on interactive cluster schedule for back to back.
"""
{
  "sync_location":"abfss://conatiner@storageaccount.dfs.core.windows.net/",
  "sync_failure_notification": "mohit.singh@databricks.com",
  "sync_success_notification": "mohit.singh@databricks.com",
  "sync_default_schedule": "* * * * *",
  "sync_max_retries": ""
  "sync_groups" : [
    {
      "group_name" : "core_product",
      "databricks_instance" : "<workflow url>"
      "group_owner_email": "mohit.singh@databricks.com",
      "workflow": "",
      "tables" : [],
    },
    {
      "group_name" : "core_product",
      "group_owner_email": "mohit.singh@databricks.com",
      "workflow": "",
      "tables" : [],
    }
  ]
}
"""
# -- config loading
  # Load above JSON config from databricks workspace directory path and parse it.
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
    
    

