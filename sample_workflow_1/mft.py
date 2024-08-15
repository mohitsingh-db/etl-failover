# Databricks notebook source
from datetime import datetime

# Paths where the sample data will be stored (replace with your actual object store paths)
path1 = "s3://one-env-uc-external-location/mohit/east/data/ingestion/"
path2 = "s3://one-env-uc-external-location/mohit/central/data/ingestion/"

# Generate a human-readable timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Define the file name with the timestamp
file_name = f"sample_file_{timestamp}.txt"

# Create the full paths by concatenating the base path and file name
file_path1 = f"{path1}{file_name}"
file_path2 = f"{path2}{file_name}"

# Create the sample content
content = f"Sample content created at {timestamp}"

# Write the content to a new file in path1 using dbutils
dbutils.fs.put(file_path1, content)

# Write the content to a new file in path2 using dbutils
dbutils.fs.put(file_path2, content)

# Print out the paths to the created files
print(f"File created in path1: {file_path1}")
print(f"File created in path2: {file_path2}")
