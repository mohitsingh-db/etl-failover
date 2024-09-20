# Lakehouse Disaster Recovery Solution 

## Background

This solution is developed to address the complexities involved in failover scenarios for ETL jobs, especially when ensuring data consistency between regions. While data replication to a secondary region can be reliable, **restarting an ETL job from the exact point of failure** is not straightforward. In many cases, users are forced to perform manual cleanup before restarting the process, which can be incredibly time-consuming and difficult, especially when strict **Recovery Time Objectives (RTO)** and **Recovery Point Objectives (RPO)** are in place.

Although it's considered a best practice to make ETL jobs **idempotent**, where they can be safely re-run without causing unintended effects, this is not always feasible in complex ETL pipelines. There are many scenarios where idempotency is difficult or impossible to achieve, and that’s where the concept of **atomicity at the job level** becomes critical. 

In this context, atomicity means treating the entire ETL workflow or job as a single **transaction**. The job's state is copied to the secondary region only when the job has reached a consistent and complete state. This allows users to start from a clean state after a failover, without needing to worry about partially processed data or manual cleanups.

#### Challenges in ETL Failover

For example, imagine an ETL workflow that updates three tables: **bronze**, **silver**, and **gold**. The silver table might undergo multiple transactions as part of its workflow, such as loops and updates to its records. If an outage occurs while the system is in the middle of updating the silver table, and if the sync job copies this **in-progress data** to the secondary region, the tables in the secondary region will be in an inconsistent state. This would make it incredibly difficult for users to rollback the partial changes or clean up the data to start from a stable point.

This is why **atomicity** is crucial. By copying the entire state of the ETL process only when it is fully complete, users can be sure that they can restart the ETL process from a clean, consistent state in the secondary region, without the risk of partial updates.

In theory, traditional approaches like **snapshotting** the data at intervals could work for some scenarios, but it becomes impractical for organizations where the boundary between **Disaster Recovery (DR)** and **High Availability (HA)** is increasingly blurred. Organizations often need a solution that provides not just recovery after a disaster but continuous availability, and that requires a more sophisticated method for ensuring data consistency.

#### Why This Solution is More Economical

Sending data from one cloud region to another incurs significant **egress costs**. One of the benefits of this solution is that it reduces those costs by only syncing data when an **atomic state** has been reached. By syncing less frequently and only when a consistent, complete state is available, we can reduce the number of bytes transferred between regions, cutting down on egress charges.

Additionally, **Spark** performs several operations on Delta tables, such as **optimize** and **vacuum**, which involve rewriting and deleting files. In a typical cloud-based sync setup, any change to underlying files (even small ones) triggers a sync operation, which means that changes unrelated to the actual data (like file rewrites) would still be transferred. This results in **duplicate data transfer** costs, even if the actual records in the tables haven't changed.

Our solution minimizes this issue by only syncing **atomic states**, meaning the sync only happens when there's a meaningful change in the data itself, rather than syncing every file modification. This approach significantly reduces egress costs and ensures that only the necessary data is transferred between regions.


## Approach

The core idea is that users can create a **group of objects** whose state is treated as a single atomic unit. In the earlier example, the entire ETL workflow and its related tables (bronze, silver, gold) can be considered as one group. The state of this group should be transferred to the secondary region only when the workflow has fully completed. In other words, **changes to the tables should be synchronized only after the entire workflow has finished**, ensuring consistency.

Simply triggering a sync at the end of the workflow would not be sufficient. This is because the next run of the workflow could start immediately, potentially modifying the tables again and causing inconsistencies in the state of the data. This is where **Delta Lake's time travel capabilities** come into play.

By leveraging **time travel**, the sync process can "travel back in time" and only copy the commits that were completed before the last workflow run. This ensures that if an outage occurs at any point during the ETL process, the secondary region can recover from a **clean and consistent state**, without copying any partial updates made during a running workflow.

In the event of an outage, when the ETL job resumes in the secondary region, it can pick up from the last consistent state, and no manual intervention or cleanup will be necessary. Time travel ensures that the secondary region’s state is atomic, and that the sync only includes data that was fully processed, avoiding any partial or inconsistent data transfers.

## Best Practices for ETL Jobs

To effectively adopt this solution, developers must follow certain best practices to ensure data consistency and smooth disaster recovery. Here are some key guidelines:

1. **Handle Backlogs Efficiently**:
   - ETL jobs should be designed to handle backlogs. For example, if an ETL job reads files from a directory and multiple files accumulate due to delays or outages, the job must be **idempotent** and able to process the backlog without reprocessing the same data incorrectly. Similarly, if an upstream table accumulates data from multiple batch runs, the ETL job should be able to handle the data either **sequentially** or **combined**, depending on the processing logic.

2. **Limit Time Travel to Current Workflow**:
   - Time travel operations in Delta Lake should be limited to the **current run** of the workflow. This ensures that the sync process captures only the relevant data committed during the last completed workflow run, avoiding partial updates from previous or subsequent workflow runs.

3. **Leverage Delta Lake and Avoid Aggressive Vacuuming**:
   - Since this solution depends on **Unity Catalog**, **Delta Lake**, and **Delta Time Travel**, it is crucial that ETL jobs are written using these features. Additionally, developers should avoid aggressive vacuuming of Delta tables. Set the **retention period** to be greater than the sync frequency to ensure that historical data required for time travel is available during the sync process.

4. **Keep the Environment in Sync**:
   - The sync process focuses exclusively on synchronizing **Delta tables**—it does not sync other objects like **workspaces, workflows, notebooks**, or configuration files. As a result, it's vital to keep the environments in sync manually. As a best practice, any changes made to critical jobs that are part of the sync process should be done through a **CI/CD pipeline**. This ensures that changes are replicated in both the primary and secondary environments simultaneously, avoiding discrepancies in workflows or environments.

By following these best practices, developers can ensure that their ETL jobs are resilient, efficient, and capable of recovering from outages while maintaining data integrity and consistency across regions.

## Disaster Recovery Architecture for Lakehouse

![Architecture Diagram](/images/architecture.png)

### Overview
This architecture diagram showcases a Disaster Recovery (DR) setup for a lakehouse, providing redundancy across two regions (Primary and Secondary). It highlights key components such as storage accounts, service principals, and the double deployment of infrastructure, with passive mode configurations for the DR region.

### Key Components:
1. **Double Deployment**:
   - All code and infrastructure need to be deployed twice, once in each region (Primary and Secondary). This is done through a CI/CD pipeline, effectively creating an additional environment.
   - The deployment in the secondary region will be in **passive mode**, where clusters are stopped, and workflows are paused until activation is necessary.

2. **Cross-Region Storage**:
   - Two separate **storage accounts** (or buckets) need to be created in both the Primary and Secondary regions. These will store:
     - **Sync metadata** 
     - **Temporary data**
   - These storage accounts need to be set up as **external locations** in the Unity Catalog metastore. 
   - If Unity Catalog is not used, proper access controls need to be established to allow access from both regions.

3. **Service Principals for Disaster Recovery**:
   - Four different **Service Principals** can be used to secure the DR process.
   - One **Service Principal (SP)** should have access to read data from the source dataset and write to the sync location in the secondary region.
   - Another **SP** should have access to read from the sync location and write to the secondary region's final destination.
   - These SPs need **read/write permissions** on both storage accounts and should be set up with proper security.

4. **Secret Management**:
   - The Service Principals need **PAT (Personal Access Tokens)**, which must be stored in **secret scopes** within Databricks for secure management.

### External Location Configuration:
- External locations in both the primary and secondary regions must be set up to handle cross-region syncing:
  1. **central_sync** (for central region sync)
  2. **east_sync** (for east region sync)
  
### Security Considerations:
- Ensure that the **Service Principals** have the required **Read/Write access** on both the primary and secondary storage accounts.
- Use **secrets** and **access tokens** for securing access credentials.


## Configuration Details:

The sync job manages the synchronization of Databricks workspaces. Each workspace has a PAT (Personal Access Token) for its Service Principal, which is stored in a secret scope. You can define **sync groups** that include specific workflows and tables to sync between the primary and secondary regions.

1. **Multiple Workspaces**:  
   - The sync job supports multiple workspaces, each with its own PAT token stored in a secret scope.

2. **Sync Groups**:  
   - Each group contains workflows and associated tables that need to be synchronized. After a workflow finishes, the sync job tracks it and syncs the tables to the secondary region using **time travel**.

3. **Standalone Tables**:  
   - For tables without workflows, the sync job performs a **deep clone** at a scheduled time to ensure they are synced to the secondary region.

4. **Notifications**:  
   - Email notifications are sent for both sync failures and successes.

5. **Sync Intervals and Retries**:  
   - You can define how often the sync happens, failover intervals, and the number of retry attempts in case of failures.

### Example Configuration:

```json
{
  "secret_scope": "dr-test-scope",
  "workspaces": [
    {
      "workspace_name": "dr_test_eastus2_databricks",
      "workspace_instance_url": "https://<workspace url>",
      "workspace_pat_key": "dr-pat-key",
      "sync_location_primary": "abfss://dr-sync@eastus2drstorageacct.dfs.core.windows.net",
      "sync_location_secondary": "abfss://dr-sync@westus2drstorageacct.dfs.core.windows.net",
      "sync_interval_mins": 15,
      "sync_failover_interval_mins": 10,
      "sync_max_retries": "3",
      "sync_groups": [
        {
          "group_name": "sample_workflow_1",
          "group_owner_email": "user@example.com",
          "workflow": "sample_workflow_1",
          "tables": [
            "dr_test_catalog.raw.workflow_1_bronze",
            "dr_test_catalog.stage.workflow_1_silver",
            "dr_test_catalog.main.workflow_1_gold"
          ]
        },
        {
          "group_name": "scheduled_sync",
          "group_owner_email": "user@example.com",
          "tables": [
            "dr_test_catalog.raw.raw_data",
            "dr_test_catalog.stage.silver_data",
            "dr_test_catalog.main.gold_data"
          ]
        }
      ]
    }
  ]
}
```


| **Property**                | **Description**                                                                                               |
|-----------------------------|---------------------------------------------------------------------------------------------------------------|
| `secret_scope`               | The secret scope in Databricks where the Personal Access Token (PAT) for the workspace is stored.             |
| `workspaces`                 | A list of workspaces that are involved in the disaster recovery sync process.                                 |
| `workspace_name`             | The name of the Databricks workspace.                                                                         |
| `workspace_instance_url`     | The URL of the Databricks workspace instance from where the sync is done.                                                               |
| `workspace_pat_key`          | The key in the secret scope that holds the PAT for accessing the workspace.                                   |
| `sync_location_primary`      | The Azure Data Lake (or S3) storage location in the primary region used to store sync metadata and intermidiate tables              |
| `sync_location_secondary`    | The Azure Data Lake (or S3) storage location in the secondary region used to store sync metadata.             |
| `sync_failure_notification`  | The email address where notifications are sent in case of sync failure.                                       |
| `sync_success_notification`  | The email address where notifications are sent when a sync is successful.                                     |
| `sync_interval_mins`         | The interval in minutes for how often the sync job runs.                                                      |
| `sync_failover_interval_mins`| The interval in minutes for retrying sync operations during failover events.                                   |
| `sync_max_retries`           | The maximum number of retries allowed if the sync job fails.                                                  |
| `sync_groups`                | A list of groups that define the workflows and tables that need to be synchronized.                           |
| `group_name`                 | The name of the sync group, which contains workflows and tables.                                              |
| `group_owner_email`          | The email address of the owner of the sync group, responsible for managing its sync operations.               |
| `workflow`                   | The workflow associated with this sync group.                                                                |
| `tables`                     | A list of tables in the catalog that need to be synchronized between the primary and secondary regions.       |


---

## Sync Job Working Overview

The sync job operates using two main components: **Sender** and **Receiver**. Both of these components are implemented as Databricks workflows.

### 1. **Sender Component**:
- The **Sender** reads the data from the primary region based on the configuration provided in the sync job.
- It periodically **deep clones** the data to the target location in the secondary region (using the sync interval).
- This process is repeated based on the specified sync intervals, ensuring that data from the primary region is regularly transferred to the secondary region.

### 2. **Receiver Component**:
- The **Receiver** reads the deep-cloned data from the target location in the secondary region.
- It then **deep clones** the data into the actual tables in the secondary region that are already registered in the **metastore**.
- These tables were initially created in the secondary region via CI/CD, ensuring consistency between regions.
  
### Key Features:
- The sync job supports both **managed tables** (Databricks-managed storage) and **external tables** (stored outside Databricks, e.g., in external storage accounts).
- During the sync process, a **metadata table** is maintained. This table:
  - Is always located in the **secondary region**.
  - Tracks the sync status and operation metrics.
  - The **Sender** updates the state and sync metrics in this metadata table, ensuring visibility into the sync process.

### Deployment: Understanding the Project Structure

1. **`dr_sync_job`**:
   - This folder contains the notebooks used to deploy the **Sender** and **Receiver** components for disaster recovery (DR).
   - If you’ve already set up the primary and secondary regions, the only step left is to deploy these jobs by creating your configuration.
   - Refer to the sample Terraform files `region_primary/dr_sync_job.tf` and `region_secondary/dr_sync_job.tf` to understand how to deploy this sync job.

2. **`region_primary`**:
   - This folder contains Terraform scripts to create:
     - A Databricks workspace.
     - A Unity Catalog metastore.
     - A storage account for sample ETL jobs.
     - A storage account for the DR sync job in the **primary region**.
   - The sync job uses this storage account to replicate data to the secondary region.

3. **`region_secondary`**:
   - This folder contains Terraform scripts to create:
     - A Databricks workspace.
     - A Unity Catalog metastore.
     - A storage account for sample ETL jobs.
     - A storage account for the DR sync job in the **secondary region**.
   - Unlike the primary region, all sample workflow jobs here are deployed in a **paused state**.

4. **`sample_workflow_1` and `sample_workflow_2`**:
   - These folders contain sample jobs demonstrating the disaster recovery capability. They are designed to show how the sync process works between regions.

5. **`file_transfer.sh`**:
   - This script is used to upload a file in both the primary and secondary regions. The uploaded file is processed by **sample workflow 1** during the ETL process.

6. **`scope_creation.sh`**:
   - The sync job requires a PAT (Personal Access Token) to check the status of workflow jobs.
   - This script helps create the necessary **secret scope** for securely storing the PAT.

By following this project structure, you can easily deploy and configure the DR sync jobs across primary and secondary regions, while using sample workflows to demonstrate the disaster recovery capabilities.