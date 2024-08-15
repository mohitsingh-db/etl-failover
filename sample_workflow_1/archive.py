# Databricks notebook source
# Paths where the file is currently stored and where it will be archived

source_path = "s3://one-env-uc-external-location/mohit/east/data/ingestion/"
archive_path = "s3://one-env-uc-external-location/mohit/east/data/archive/"

# Specify the file name (if known) or list and process files dynamically
# Example: file_name = "sample_file_20240813_154530.txt"

# Optionally, list files in the source path (if you want to move all files)
files = dbutils.fs.ls(source_path)

for file_info in files:
    file_name = file_info.name  # Extract the file name
    source_file_path = f"{source_path}{file_name}"
    archive_file_path = f"{archive_path}{file_name}"
    
    # Move the file by copying it to the archive path and then deleting it from the source path
    dbutils.fs.cp(source_file_path, archive_file_path)
    dbutils.fs.rm(source_file_path)

    # Print confirmation
    print(f"File moved from {source_file_path} to {archive_file_path}")