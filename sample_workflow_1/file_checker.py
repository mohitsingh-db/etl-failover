# Databricks notebook source
# Define the source directory path
source_directory = "s3://one-env-uc-external-location/mohit/east/data/ingestion/"  # Replace with your actual S3 path

# Check if any files exist in the source directory
files = dbutils.fs.ls(source_directory)

# Set the task status based on the presence of files
if files:
    dbutils.jobs.taskValues.set("file_check_status", "true")
    print("Files found, setting status to FILES_FOUND.")
else:
    dbutils.jobs.taskValues.set("file_check_status", "false")
    print("No files found, setting status to NO_FILES_FOUND.")
