import time

def process_sync_job(config: dict):
    # Loop through each entry in metadata table
    for group in config['sync_groups']:
        # Check dependent workflow status
        workflow_status = check_workflow_status(group['workflow'])
        if workflow_status == 'success':
            sync_start_timestamp = time.time()
            update_metadata_status(group['group_name'], 'in-progress', '')

            # Perform sync operation
            try:
                sync_tables(group['tables'], config['sync_location'])
                update_metadata_status(group['group_name'], 'success', '')
            except Exception as e:
                update_metadata_status(group['group_name'], 'failed', str(e))
        else:
            update_metadata_status(group['group_name'], 'failed', 'Dependent workflow not completed successfully')
