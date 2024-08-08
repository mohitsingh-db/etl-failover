from config_loader import load_config
from validator import validate_config, validate_workflow_and_tables
from metadata_manager import create_or_update_metadata
from processor import process_sync_job

def main():
    config_file_path = '/dbfs/path/to/config.json'
    config = load_config(config_file_path)

    if validate_config(config) and validate_workflow_and_tables(config):
        create_or_update_metadata(config)
        process_sync_job(config)

if __name__ == '__main__':
    main()
